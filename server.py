"""Web side: aiohttp routes and the Telegram byte streamer (parallel chunk prefetch)."""
import asyncio
import json
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

from bot import FIleNotFound, FileStream, StartTime, clean_filename, db, humanbytes, uptime
from config import Player, Server, Streaming, Telegram, base_url

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


async def stream_chunks(fid: FileId, offset: int, first_cut: int, last_cut: int, parts: int):
    """Yields file bytes. Keeps several chunk requests in flight so Telegram latency and the
    client's download speed overlap. Few streams -> deep prefetch (fast); many streams ->
    shallower prefetch so total buffered memory stays under BUFFER_CHUNKS."""
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
        file_name=doc["file_name"].replace("_", " "),
        file_url=f"{base_url()}dl/{db_id}",
        file_size=humanbytes(doc["file_size"]),
    )
    return web.Response(text=html, content_type="text/html")


mediainfo_cache = {}  # instant subsequent loads (capped)

@routes.get("/mediainfo/{path}", allow_head=True)
async def mediainfo_route_handler(request: web.Request):
    async with MEDIAINFO_SEM:  # keep these CPU-heavy reports from starving streams
        return await _mediainfo(request)


async def _mediainfo(request: web.Request):
    try:
        path = request.match_info["path"]
        
        if path in mediainfo_cache:
            raw_info = mediainfo_cache[path]
        else:
            local_url = f"http://127.0.0.1:{Server.PORT}/dl/{path}"
            
            proc = await asyncio.create_subprocess_exec(
                "mediainfo",
                local_url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, _ = await asyncio.wait_for(
                    proc.communicate(), timeout=20
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                stdout = b""

            if proc.returncode == 0 and stdout:
                raw_info = stdout.decode("utf-8", errors="ignore")
                if len(mediainfo_cache) >= 200:
                    mediainfo_cache.pop(next(iter(mediainfo_cache)))
                mediainfo_cache[path] = raw_info
            else:
                raw_info = "Unable to extract MediaInfo."

        # --- CLEANING THE RAW REPORT ---
        lines = []
        for line in raw_info.splitlines():
            stripped = line.strip()
            if (stripped.startswith("Title") or 
                stripped.startswith("Movie name") or 
                stripped.startswith("Writing application") or 
                stripped.startswith("Writing library")):
                continue
            if stripped.startswith("Text #") or stripped == "Text":
                line = line.replace("Text", "Subtitle")
            lines.append(line)
        cleaned_info = "\n".join(lines)

        # --- DYNAMIC EXTRACTION FOR QUICK SUMMARY ---
        def extract(pattern, text, default="N/A"):
            match = re.search(pattern, text)
            return match.group(1).strip() if match else default

        file_size = extract(r"File size\s*:\s*(.*)", raw_info)
        duration = extract(r"Duration\s*:\s*(.*)", raw_info)
        
        v_width_str = extract(r"Width\s*:\s*([\d\s]+pixels)", raw_info).replace(" ", "").replace("pixels", "")
        v_height_str = extract(r"Height\s*:\s*([\d\s]+pixels)", raw_info).replace(" ", "").replace("pixels", "")
        
        if v_width_str.isdigit() and v_height_str.isdigit():
            w, h = int(v_width_str), int(v_height_str)
            resolution = f"{w}x{h}"
        else:
            resolution = "N/A"
        
        video_block_match = re.search(r"Video\n(.*?)(?=\n\n|\nAudio|\nSubtitle|\nText|\Z)", raw_info, re.DOTALL)
        video_block = video_block_match.group(1) if video_block_match else raw_info
        video_codec = extract(r"Format\s*:\s*(.*)", video_block, "AVC / HEVC")

        raw_bit_depth = extract(r"Bit depth\s*:\s*(.*)", video_block, "")
        bit_depth = raw_bit_depth.replace("bits", "").replace("bit", "").strip()
        bit_depth = f"{bit_depth}-bit" if bit_depth.isdigit() else "N/A"

        # Fetch cleaned file name from the database for the header
        display_filename = "Media File"
        try:
            file_info = await db.get_file(path)
            display_filename = clean_filename(file_info.get("file_name", "")) or display_filename
        except Exception:
            pass

        # Strict Audio Language Extraction
        audio_langs = re.findall(r"Audio\s*#?\d*\n(?:[^\n]+\n)*?.*?Language\s*:\s*(.*)", raw_info)
        if not audio_langs:
            audio_langs = re.findall(r"Language\s*:\s*(.*)", raw_info)
        valid_audio = [l.strip() for l in audio_langs if l.strip().lower() not in ['default', 'forced', 'no']]
        audio_str = ", ".join(dict.fromkeys(valid_audio)) if valid_audio else "None"

        # Strict Subtitle Track and Language Detection
        sub_blocks = re.findall(r"(?:Subtitle|Text)\s*#?\d*\n(?:[^\n]+\n)*?.*?Language\s*:\s*(.*)", raw_info, re.IGNORECASE)
        if not sub_blocks:
            sub_count = len(re.findall(r"\b(Subtitle|Text)\b", raw_info))
            sub_str = "Present (No Language)" if sub_count > 0 else "None"
        else:
            valid_subs = [l.strip() for l in sub_blocks if l.strip().lower() not in ['default', 'forced', 'no']]
            sub_str = ", ".join(dict.fromkeys(valid_subs)) if valid_subs else "None"

        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>MediaInfo Report</title>
            <style>
                body {{
                    background: #0d1117;
                    color: #c9d1d9;
                    font-family: 'Courier New', Courier, monospace;
                    padding: 15px;
                    margin: 0;
                    box-sizing: border-box;
                }}
                .file-header {{
                    background: #161b22;
                    border: 1px solid #30363d;
                    padding: 12px 18px;
                    border-radius: 8px;
                    margin-bottom: 15px;
                    font-size: 14px;
                    color: #ff7b72;
                    font-weight: bold;
                    word-break: break-all;
                }}
                .summary-card {{
                    background: #161b22;
                    border: 1px solid #30363d;
                    border-left: 5px solid #58a6ff;
                    padding: 15px;
                    border-radius: 8px;
                    margin-bottom: 25px;
                }}
                .summary-card h3 {{
                    margin-top: 0;
                    color: #58a6ff;
                }}
                .summary-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 10px;
                    font-size: 13px;
                }}
                .tag-video {{ color: #3fb950; font-weight: bold; }}
                .tag-audio {{ color: #f0883e; font-weight: bold; }}
                .tag-sub {{ color: #bc8cff; font-weight: bold; }}
                .tag-gen {{ color: #58a6ff; font-weight: bold; }}

                h2 {{
                    color: #58a6ff;
                    border-bottom: 2px solid #30363d;
                    padding-bottom: 8px;
                    font-size: 18px;
                }}
                pre {{
                    background: #161b22;
                    padding: 15px;
                    border-radius: 10px;
                    overflow-x: auto;
                    white-space: pre-wrap;
                    word-wrap: break-word;
                    border: 1px solid #30363d;
                    font-size: 12px;
                    line-height: 1.4;
                }}
                .sec-header {{
                    color: #79c0ff;
                    font-size: 15px;
                    font-weight: bold;
                    display: block;
                    margin-top: 15px;
                    margin-bottom: 5px;
                    border-bottom: 1px dashed #30363d;
                    padding-bottom: 3px;
                }}
                .sec-general {{ color: #58a6ff; }}
                .sec-video {{ color: #3fb950; }}
                .sec-audio {{ color: #f0883e; }}
                .sec-sub {{ color: #bc8cff; }}
            </style>
        </head>
        <body>
            <div class="file-header">
                🔆 [ {file_size} ] {display_filename}
            </div>

            <div class="summary-card">
                <h3>⚡ Quick Media Summary</h3>
                <div class="summary-grid">
                    <div>🎬 <b>Resolution:</b> <span class="tag-video">{resolution}</span></div>
                    <div>🎞️ <b>Video Codec:</b> <span class="tag-video">{video_codec}</span></div>
                    <div>🎨 <b>Bit Depth:</b> <span class="tag-video">{bit_depth}</span></div>
                    <div>⏱️ <b>Duration:</b> <span class="tag-gen">{duration}</span></div>
                    <div>📦 <b>File Size:</b> <span class="tag-gen">{file_size}</span></div>
                    <div>🔊 <b>Audio Tracks:</b> <span class="tag-audio">{audio_str}</span></div>
                    <div>💬 <b>Subtitles:</b> <span class="tag-sub">{sub_str}</span></div>
                </div>
            </div>

            <h2>📄 Full Technical Metadata</h2>
            <pre><code id="raw-report">{cleaned_info}</code></pre>

            <script>
                document.addEventListener("DOMContentLoaded", function() {{
                    const codeEl = document.getElementById("raw-report");
                    let lines = codeEl.innerHTML.split("\\n");
                    let currentClass = "sec-general";

                    let coloredLines = lines.map(line => {{
                        let trimmed = line.trim();
                        if (trimmed === "General") {{
                            currentClass = "sec-general";
                            return `<span class="sec-header">📊 General</span>`;
                        }} else if (trimmed === "Video") {{
                            currentClass = "sec-video";
                            return `<span class="sec-header">🎬 Video</span>`;
                        }} else if (trimmed.startsWith("Audio")) {{
                            currentClass = "sec-audio";
                            return `<span class="sec-header">🔊 ${{line}}</span>`;
                        }} else if (trimmed.startsWith("Subtitle")) {{
                            currentClass = "sec-sub";
                            return `<span class="sec-header">💬 ${{line}}</span>`;
                        }}

                        return `<span class="${{currentClass}}">${{line}}</span>`;
                    }});

                    codeEl.innerHTML = coloredLines.join("\\n");
                }});
            </script>
        </body>
        </html>
        """
        return web.Response(text=html, content_type="text/html")
    except Exception as e:
        return web.Response(text=f"Error generating MediaInfo: {str(e)}", status=500)



# ====================================================================
# AUDIO / SUBTITLE TRACKS (ffmpeg, lowest CPU priority, strictly limited)
#   /tracks/<id>        audio + subtitle track list (ffprobe, cached)
#   /sub/<id>/<n>       subtitle track n as WebVTT (extracted once, cached)
#   /seekpoint/<id>?t=  nearest keyframe at/before t (keeps subtitles in sync after a seek)
#   /audio/<id>?a=&t=   video copied as-is + audio track a re-encoded, as a fragmented mp4 stream
# The default audio keeps using the plain /dl stream (no ffmpeg at all).
# ====================================================================
NICE = shutil.which("nice")
PROBE_SEM = asyncio.Semaphore(2)
EXTRACT_SEM = asyncio.Semaphore(Player.EXTRACT_LIMIT)
REMUX_SEM = asyncio.Semaphore(Player.REMUX_LIMIT)
TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text", "microdvd", "subviewer", "sami"}
_tracks_cache: dict = {}
_sub_cache: dict = {}
_sub_jobs: dict = {}


def local_url(db_id: str) -> str:
    return f"http://127.0.0.1:{Server.PORT}/dl/{db_id}"


def low_priority(cmd: list) -> list:
    return [NICE, "-n", "19"] + cmd if NICE else cmd


def probe_cmd(url):
    return ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_format", url]


def parse_tracks(data: dict) -> dict:
    audio, subs, vcodec = [], [], ""
    for s in data.get("streams", []):
        tags = {k.lower(): v for k, v in (s.get("tags") or {}).items()}
        kind = s.get("codec_type")
        item = {
            "lang": tags.get("language", ""),
            "title": tags.get("title", ""),
            "codec": s.get("codec_name", ""),
            "default": bool((s.get("disposition") or {}).get("default")),
        }
        if kind == "audio":
            audio.append({**item, "n": len(audio), "channels": s.get("channels", 0)})
        elif kind == "subtitle":
            subs.append({**item, "n": len(subs), "text": s.get("codec_name") in TEXT_SUB_CODECS})
        elif kind == "video" and not vcodec and not (s.get("disposition") or {}).get("attached_pic"):
            vcodec = s.get("codec_name", "")
    try:
        duration = float((data.get("format") or {}).get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0
    return {"duration": duration, "vcodec": vcodec, "audio": audio, "subs": subs}


def seekpoint_cmd(url, t: float):
    return ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=pts_time,flags",
            "-read_intervals", f"{t:.3f}%+#1", "-print_format", "json", url]


def parse_seekpoint(data: dict, fallback: float) -> float:
    try:
        return min(float(data["packets"][0]["pts_time"]), fallback)
    except (KeyError, IndexError, TypeError, ValueError):
        return fallback


def sub_cmd(url, n: int):
    return ["ffmpeg", "-v", "error", "-nostdin", "-i", url, "-map", f"0:s:{n}", "-f", "webvtt", "-"]


def remux_cmd(url, a: int, start: float):
    # +0.01: land ON the keyframe reported by /seekpoint, not the one before it
    return [
        "ffmpeg", "-v", "error", "-nostdin", "-probesize", "5M", "-analyzeduration", "5M",
        "-ss", f"{start + 0.01:.3f}", "-i", url,
        "-map", "0:V:0", "-map", f"0:a:{a}",
        "-c:v", "copy", "-c:a", Player.AUDIO_CODEC, "-b:a", Player.AUDIO_BITRATE, "-ac", "2",
        "-sn", "-dn", "-flush_packets", "1",
        "-f", "mp4", "-movflags", "frag_keyframe+empty_moov+default_base_moof", "pipe:1",
    ]


async def run_capture(cmd: list, timeout: float):
    proc = await asyncio.create_subprocess_exec(
        *low_priority(cmd), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        return proc.returncode, out
    except BaseException:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        asyncio.create_task(proc.wait())
        raise


def _retrieved(task):
    task.cancelled() or task.exception()


async def _require_file(db_id: str):
    try:
        await get_file_id(db_id)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)


def _num(request, key, cast, default):
    try:
        return cast(request.query.get(key, default))
    except ValueError:
        raise web.HTTPBadRequest(text=f"bad {key}")


@routes.get("/tracks/{path}")
async def tracks_route(request: web.Request):
    db_id = request.match_info["path"]
    await _require_file(db_id)
    info = _tracks_cache.get(db_id)
    if info is None:
        try:
            async with PROBE_SEM:
                rc, out = await run_capture(probe_cmd(local_url(db_id)), 40)
            if rc != 0:
                raise RuntimeError(f"ffprobe exit {rc}")
            info = parse_tracks(json.loads(out))
            if len(_tracks_cache) > 200:
                _tracks_cache.clear()
            _tracks_cache[db_id] = info
        except Exception as e:  # player just hides the buttons
            logging.warning(f"track probe failed for {db_id}: {e!r}")
            info = {"duration": 0, "vcodec": "", "audio": [], "subs": []}
    return web.json_response(info)


@routes.get("/seekpoint/{path}")
async def seekpoint_route(request: web.Request):
    db_id = request.match_info["path"]
    t = max(0.0, _num(request, "t", float, "0"))
    await _require_file(db_id)
    start = t
    if t > 0:
        try:
            async with PROBE_SEM:
                rc, out = await run_capture(seekpoint_cmd(local_url(db_id), t), 30)
            if rc == 0 and out:
                start = parse_seekpoint(json.loads(out), t)
        except Exception as e:
            logging.warning(f"seekpoint failed for {db_id}: {e!r}")
    return web.json_response({"start": round(start, 3)})


async def _extract_sub(db_id: str, n: int) -> str:
    async with EXTRACT_SEM:
        rc, out = await run_capture(sub_cmd(local_url(db_id), n), 600)
    if rc != 0 or not out.strip():
        raise RuntimeError(f"subtitle extraction failed (exit {rc})")
    text = out.decode("utf-8", "replace")
    if len(_sub_cache) >= 40:
        _sub_cache.pop(next(iter(_sub_cache)))
    _sub_cache[(db_id, n)] = text
    return text


@routes.get("/sub/{path}/{n}")
async def sub_route(request: web.Request):
    db_id = request.match_info["path"]
    try:
        n = int(request.match_info["n"])
    except ValueError:
        raise web.HTTPBadRequest(text="bad track")
    await _require_file(db_id)
    key = (db_id, n)
    text = _sub_cache.get(key)
    if text is None:
        job = _sub_jobs.get(key)
        if job is None:  # many viewers asking at once share one extraction
            job = _sub_jobs[key] = asyncio.create_task(_extract_sub(db_id, n))
            job.add_done_callback(lambda _t, k=key: _sub_jobs.pop(k, None))
            job.add_done_callback(_retrieved)
        try:
            text = await asyncio.shield(job)
        except Exception as e:
            logging.warning(f"subtitle {n} for {db_id}: {e!r}")
            raise web.HTTPNotFound(text="Subtitle unavailable")
    return web.Response(
        text=text, content_type="text/vtt", charset="utf-8", headers={"Cache-Control": "public, max-age=3600"}
    )


@routes.get("/audio/{path}")
async def audio_route(request: web.Request):
    db_id = request.match_info["path"]
    a = _num(request, "a", int, "0")
    start = max(0.0, _num(request, "t", float, "0"))
    await _require_file(db_id)
    try:
        await asyncio.wait_for(REMUX_SEM.acquire(), timeout=Streaming.QUEUE_WAIT)
    except asyncio.TimeoutError:
        raise web.HTTPServiceUnavailable(text="Audio switching is busy, retry shortly", headers={"Retry-After": "5"})
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *low_priority(remux_cmd(local_url(db_id), a, start)),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        response = web.StreamResponse(
            status=200,
            headers={"Content-Type": "video/mp4", "Cache-Control": "no-store", "Accept-Ranges": "none"},
        )
        await response.prepare(request)
        try:
            while True:
                chunk = await proc.stdout.read(256 * 1024)
                if not chunk:
                    break
                await response.write(chunk)
            await response.write_eof()
        except (ConnectionResetError, BrokenPipeError):
            pass
        return response
    finally:
        if proc is not None:
            if proc.returncode is None:
                proc.kill()
            asyncio.create_task(proc.wait())
        REMUX_SEM.release()


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
