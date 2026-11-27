"""Web side: aiohttp routes and the Telegram byte streamer (parallel chunk prefetch)."""
import asyncio
import logging
import mimetypes
import os
import re
import shutil
import time
import urllib.parse
from collections import deque
from contextlib import aclosing

import jinja2
from aiohttp import web
from pyrogram import raw
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId
from pyrogram.session import Auth, Session

from bot import FIleNotFound, FileStream, StartTime, db, display_name, uptime
from config import Server, Streaming, Telegram, base_url

CHUNK = 1024 * 1024  # Telegram's max GetFile size
FILE_ID_TTL = Streaming.FILE_ID_TTL

STREAM_SEM = asyncio.Semaphore(Streaming.MAX_STREAMS)
MEDIAINFO_SEM = asyncio.Semaphore(Streaming.MEDIAINFO_LIMIT)
active_streams = 0

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "play.html")) as _f:
    PLAY_TEMPLATE = jinja2.Template(_f.read())


# ====================================================================
# TELEGRAM FILE LOOKUP + MEDIA SESSION
# ====================================================================
_file_ids: dict = {}   # db_id -> (FileId, expires_at)
_dc_locks: dict = {}   # dc_id -> Lock (stops duplicate session creation)


async def get_file_id(db_id: str) -> FileId:
    """FileId for a stored file. Fresh one is read from the log-channel message (file references
    expire), cached for FILE_ID_TTL. Old records without log_msg_id use their stored ids."""
    hit = _file_ids.get(db_id)
    if hit and hit[1] > time.time():
        return hit[0]

    doc = await db.get_file(db_id)
    encoded = None
    if doc.get("log_msg_id"):
        msg = await FileStream.get_messages(Telegram.FLOG_CHANNEL, doc["log_msg_id"])
        media = (msg.video or msg.document) if msg and not msg.empty else None
        if not media:
            raise FIleNotFound
        encoded = media.file_id
    elif doc.get("file_ids"):
        encoded = doc["file_ids"].get(str(FileStream.id))
    if not encoded:
        raise FIleNotFound

    fid = FileId.decode(encoded)
    fid.file_size = doc.get("file_size", 0)
    fid.mime_type = doc.get("mime_type") or "application/octet-stream"
    fid.file_name = doc.get("file_name") or "file"

    if len(_file_ids) > 1000:
        _file_ids.clear()
    _file_ids[db_id] = (fid, time.time() + FILE_ID_TTL)
    return fid


async def get_media_session(dc_id: int) -> Session:
    sess = FileStream.media_sessions.get(dc_id)
    if sess:
        return sess
    async with _dc_locks.setdefault(dc_id, asyncio.Lock()):
        sess = FileStream.media_sessions.get(dc_id)
        if sess:
            return sess
        test_mode = await FileStream.storage.test_mode()
        if dc_id != await FileStream.storage.dc_id():
            sess = Session(
                FileStream, dc_id, await Auth(FileStream, dc_id, test_mode).create(), test_mode, is_media=True
            )
            await sess.start()
            for _ in range(6):
                exported = await FileStream.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                try:
                    await sess.invoke(raw.functions.auth.ImportAuthorization(id=exported.id, bytes=exported.bytes))
                    break
                except AuthBytesInvalid:
                    continue
            else:
                await sess.stop()
                raise AuthBytesInvalid
        else:
            sess = Session(FileStream, dc_id, await FileStream.storage.auth_key(), test_mode, is_media=True)
            await sess.start()
        FileStream.media_sessions[dc_id] = sess
        return sess


async def fetch_chunk(fid: FileId, location, offset: int) -> bytes:
    """One GetFile call; on failure replaces a dead media session and retries once."""
    for attempt in (1, 2):
        sess = await get_media_session(fid.dc_id)
        try:
            r = await asyncio.wait_for(
                sess.invoke(raw.functions.upload.GetFile(location=location, offset=offset, limit=CHUNK)),
                timeout=30,
            )
            return r.bytes if isinstance(r, raw.types.upload.File) else b""
        except Exception as e:
            if attempt == 2:
                raise
            logging.warning(f"GetFile failed on DC {fid.dc_id} ({e!r}); recreating media session")
            if FileStream.media_sessions.get(fid.dc_id) is sess:  # only the first failer replaces it
                FileStream.media_sessions.pop(fid.dc_id, None)
                asyncio.create_task(_stop_quietly(sess))


async def _stop_quietly(sess):
    try:
        await sess.stop()
    except Exception:
        pass


async def stream_chunks(fid: FileId, offset: int, first_cut: int, last_cut: int, parts: int, prefetch=None):
    """Yields file bytes. Keeps several chunk requests in flight so Telegram latency and the
    client's download speed overlap. Few streams -> deep prefetch (fast); many streams ->
    shallower prefetch so total buffered memory stays under BUFFER_CHUNKS."""
    if prefetch is None:
        prefetch = max(1, min(Streaming.PREFETCH, Streaming.BUFFER_CHUNKS // max(active_streams, 1)))
    location = raw.types.InputDocumentFileLocation(
        id=fid.media_id,
        access_hash=fid.access_hash,
        file_reference=fid.file_reference,
        thumb_size=fid.thumbnail_size,
    )
    pending, nxt = deque(), 0

    def fill():
        nonlocal nxt
        while len(pending) < prefetch and nxt < parts:
            t = asyncio.create_task(fetch_chunk(fid, location, offset + nxt * CHUNK))
            t.add_done_callback(lambda t: t.cancelled() or t.exception())  # mark retrieved
            pending.append(t)
            nxt += 1

    try:
        fill()
        for i in range(parts):
            data = await pending.popleft()
            fill()
            if not data:
                break
            if parts == 1:
                yield data[first_cut:last_cut]
            elif i == 0:
                yield data[first_cut:]
            elif i == parts - 1:
                yield data[:last_cut]
            else:
                yield data
    finally:
        for t in pending:
            t.cancel()


# ====================================================================
# MIDDLEWARE + ROUTES
# ====================================================================
@web.middleware
async def request_logger(request, handler):
    t0, status = time.monotonic(), "ERR"
    try:
        resp = await handler(request)
        status = resp.status
        return resp
    except web.HTTPException as e:
        status = e.status
        raise
    finally:
        logging.info(
            f"[REQ] {request.method} {request.path} -> {status} "
            f"{(time.monotonic() - t0) * 1000:.0f}ms "
            f"ip={request.headers.get('X-Forwarded-For', request.remote)} "
            f"range={request.headers.get('Range', '-')}"
        )


routes = web.RouteTableDef()


@routes.get("/")
async def root(request):
    return web.Response(text="Bot is running!")


@routes.get("/status", allow_head=True)
async def status(_):
    return web.json_response(
        {
            "server_status": "running",
            "uptime": uptime(time.time() - StartTime),
            "telegram_bot": "@" + str(FileStream.username),
            "active_streams": active_streams,
            "max_streams": Streaming.MAX_STREAMS,
        }
    )


@routes.get("/watch/{path}", allow_head=True)
async def watch(request: web.Request):
    db_id = request.match_info["path"]
    try:
        doc = await db.get_file(db_id)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    if not doc["mime_type"].startswith("video"):
        raise web.HTTPFound(f"/dl/{db_id}")
    html = PLAY_TEMPLATE.render(
        title=display_name(doc["file_name"], doc["file_size"]),
        file_url=f"{base_url()}dl/{db_id}",
        summary_url=f"{base_url()}summary/{db_id}",
        info_url=f"{base_url()}mediainfo/{db_id}",
    )
    return web.Response(text=html, content_type="text/html")


# ====================================================================
# MEDIAINFO (runs `mediainfo` once per file, lowest CPU priority, cached in memory + MongoDB)
#   /summary/<id>   the "Quick Media Summary" as JSON (the web player fetches it after playback starts)
#   /mediainfo/<id> full report page
# ====================================================================
NICE = shutil.which("nice")
mediainfo_cache: dict = {}   # id -> raw mediainfo text (capped)
summary_cache: dict = {}     # id -> summary dict (capped)
_summary_jobs: dict = {}     # id -> running task (many viewers of one file share one run)
SKIP_LINES = ("Title", "Movie name", "Writing application", "Writing library")


def _cap(cache: dict, key, value, cap: int = 200):
    if len(cache) >= cap:
        cache.pop(next(iter(cache)))
    cache[key] = value


async def run_mediainfo(db_id: str) -> str:
    cmd = ["mediainfo", f"http://127.0.0.1:{Server.PORT}/dl/{db_id}"]
    if NICE:
        cmd = [NICE, "-n", "15"] + cmd
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), 25)
    except BaseException:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        asyncio.create_task(proc.wait())
        raise
    if proc.returncode != 0 or not out:
        raise RuntimeError("mediainfo returned nothing")
    text = "\n".join(l for l in out.decode("utf-8", "ignore").splitlines() if not l.strip().startswith(SKIP_LINES))
    return text.replace("\nText", "\nSubtitle")


def _sections(raw_info: str):
    """-> list of (kind, {field: value}) for General / Video / Audio / Text blocks."""
    out = []
    for block in re.split(r"\n\s*\n", raw_info.strip()):
        lines = block.strip().splitlines()
        if not lines:
            continue
        kind = re.match(r"(General|Video|Audio|Subtitle|Text|Menu|Other|Image)\b", lines[0].strip())
        if not kind:
            continue
        fields = {}
        for ln in lines[1:]:
            k, _, v = ln.partition(":")
            if v:
                fields.setdefault(k.strip(), v.strip())
        out.append((kind.group(1), fields))
    return out


def parse_summary(raw_info: str) -> dict:
    secs = _sections(raw_info)
    gen = next((f for k, f in secs if k == "General"), {})
    vid = next((f for k, f in secs if k == "Video"), {})

    def langs(kind):
        seen = []
        for k, f in secs:
            if k == kind:
                name = f.get("Language", "").split("(")[0].strip()
                if name and name.lower() not in ("default", "forced", "no") and name not in seen:
                    seen.append(name)
        return seen

    def num(s):
        return re.sub(r"\D", "", s)

    w, h = num(vid.get("Width", "")), num(vid.get("Height", ""))
    depth = num(vid.get("Bit depth", ""))
    n_subs = sum(1 for k, _ in secs if k in ("Subtitle", "Text"))
    subs = langs("Subtitle") + [x for x in langs("Text") if x not in langs("Subtitle")]
    return {
        "resolution": f"{w}x{h}" if w and h else "",
        "codec": vid.get("Format", ""),
        "bit_depth": f"{depth}-bit" if depth else "",
        "duration": gen.get("Duration", ""),
        "size": gen.get("File size", ""),
        "audio": ", ".join(langs("Audio")),
        "subs": ", ".join(subs) if subs else ("Available" if n_subs else "None"),
    }


async def _summary_job(db_id: str) -> dict:
    async with MEDIAINFO_SEM:  # these are CPU heavy: one at a time, never ahead of streams
        text = await run_mediainfo(db_id)
    s = parse_summary(text)
    _cap(mediainfo_cache, db_id, text)
    _cap(summary_cache, db_id, s)
    try:
        await asyncio.wait_for(db.file.database["media"].replace_one({"_id": db_id}, {"_id": db_id, **s}, upsert=True), 5)
    except Exception:
        pass
    return s


async def get_summary(db_id: str) -> dict:
    if db_id in summary_cache:
        return summary_cache[db_id]
    try:
        doc = await asyncio.wait_for(db.file.database["media"].find_one({"_id": db_id}), 3)
        if doc:
            doc.pop("_id", None)
            _cap(summary_cache, db_id, doc)
            return doc
    except Exception:
        pass
    task = _summary_jobs.get(db_id)
    if task is None:
        task = _summary_jobs[db_id] = asyncio.create_task(_summary_job(db_id))
        task.add_done_callback(lambda t, k=db_id: (_summary_jobs.pop(k, None), t.cancelled() or t.exception()))
    return await asyncio.shield(task)


@routes.get("/summary/{path}")
async def summary_route(request: web.Request):
    db_id = request.match_info["path"]
    try:
        await db.get_file(db_id)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    try:
        return web.json_response(await get_summary(db_id), headers={"Cache-Control": "public, max-age=3600"})
    except Exception as e:
        logging.warning(f"summary failed for {db_id}: {e!r}")
        return web.json_response({"error": True}, status=503)


MI_PAGE = jinja2.Template("""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>MediaInfo</title>
<style>
body{margin:0;background:#0c0d0f;color:#d7dbe0;font:14px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.w{max-width:860px;margin:0 auto;padding:18px 14px 40px}
h1{font-size:16px;font-weight:600;margin:0 0 14px;word-break:break-word}
.card{background:#16181b;border:1px solid #2a2e33;border-radius:12px;padding:14px 16px;margin-bottom:16px}
.card h2{font-size:15px;margin:0 0 8px}.card div{padding:2px 0}
pre{background:#16181b;border:1px solid #2a2e33;border-radius:12px;padding:14px;overflow-x:auto;white-space:pre-wrap;word-break:break-word;font-size:12.5px;line-height:1.45}
</style></head><body><div class="w">
<h1>🔆 {{ title }}</h1>
<div class="card"><h2>⚡ Quick Media Summary</h2>
<div>🎬 Resolution: {{ s.resolution or "N/A" }}</div><div>🎞️ Video Codec: {{ s.codec or "N/A" }}</div>
<div>🎨 Bit Depth: {{ s.bit_depth or "N/A" }}</div><div>⏱️ Duration: {{ s.duration or "N/A" }}</div>
<div>📦 File Size: {{ s.size or "N/A" }}</div><div>🔊 Audio Tracks: {{ s.audio or "N/A" }}</div>
<div>💬 Subtitles: {{ s.subs or "None" }}</div></div>
<pre>{{ raw }}</pre></div></body></html>""", autoescape=True)


@routes.get("/mediainfo/{path}", allow_head=True)
async def mediainfo_route(request: web.Request):
    db_id = request.match_info["path"]
    try:
        doc = await db.get_file(db_id)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    try:
        s = await get_summary(db_id)
        if db_id not in mediainfo_cache:  # summary came from MongoDB: the full text needs one more run
            async with MEDIAINFO_SEM:
                _cap(mediainfo_cache, db_id, await run_mediainfo(db_id))
        raw_text = mediainfo_cache[db_id]
    except Exception as e:
        return web.Response(text=f"Could not read this file's MediaInfo right now ({e}). Try again in a moment.", status=503)
    title = display_name(doc["file_name"], doc["file_size"])
    return web.Response(text=MI_PAGE.render(title=title, s=s, raw=raw_text), content_type="text/html")


@routes.get("/dl/{path}", allow_head=True)
async def dl(request: web.Request):
    global active_streams
    db_id = request.match_info["path"]
    try:
        fid = await get_file_id(db_id)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)

    size = fid.file_size
    start, end, status = 0, size - 1, 200
    rng = request.headers.get("Range", "").strip()
    m = re.fullmatch(r"bytes=(\d*)-(\d*)", rng)
    if m and (m[1] or m[2]):
        if m[1]:
            start = int(m[1])
            end = min(int(m[2]), size - 1) if m[2] else size - 1
        else:  # suffix range: last N bytes
            start = max(size - int(m[2]), 0)
        status = 206
        if start >= size or start > end:
            return web.Response(status=416, headers={"Content-Range": f"bytes */{size}"})

    name = fid.file_name
    ascii_name = name.encode("ascii", "ignore").decode().replace('"', "") or "file"
    headers = {
        "Content-Type": fid.mime_type or mimetypes.guess_type(name)[0] or "application/octet-stream",
        "Content-Length": str(end - start + 1),
        "Content-Disposition": f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{urllib.parse.quote(name)}",
        "Accept-Ranges": "bytes",
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    if request.method == "HEAD":
        return web.Response(status=status, headers=headers)

    # Queue briefly for a free slot instead of failing instantly.
    try:
        await asyncio.wait_for(STREAM_SEM.acquire(), timeout=Streaming.QUEUE_WAIT)
    except asyncio.TimeoutError:
        raise web.HTTPServiceUnavailable(text="Server busy, retry shortly", headers={"Retry-After": "3"})

    active_streams += 1
    try:
        offset = start - (start % CHUNK)
        parts = end // CHUNK - offset // CHUNK + 1
        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)
        try:
            async with aclosing(stream_chunks(fid, offset, start - offset, end % CHUNK + 1, parts)) as chunks:
                async for chunk in chunks:
                    await response.write(chunk)
            await response.write_eof()
        except (ConnectionResetError, BrokenPipeError):
            pass  # client went away
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"Stream of {name} failed: {e!r}")
            _file_ids.pop(db_id, None)  # force a fresh file id next time
            if request.transport:
                request.transport.close()
        return response
    finally:
        active_streams -= 1
        STREAM_SEM.release()


web_app = web.Application(client_max_size=30000000, middlewares=[request_logger])
web_app.add_routes(routes)
