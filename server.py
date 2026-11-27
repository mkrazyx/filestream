"""Web side: aiohttp routes and the Telegram byte streamer (parallel chunk prefetch)."""
import asyncio
import base64
import bisect
import html
import json
import logging
import mimetypes
import os
import re
import shutil
import struct
import tempfile
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
# MEDIA ANALYSIS, AUDIO TRACKS, SUBTITLES
#   /info/<id>     one call: audio + subtitle track list, keyframe index, subtitle progress
#   /subs/<id>     every text subtitle track as ready-to-show cues (extracted once, then cached)
#   /audio/<id>    video copied as-is + the chosen audio track, as a fragmented mp4 stream
#   /seekpoint     fallback for non-MKV files only (nearest keyframe)
# MKV files are read in-process (no ffprobe): track list and keyframe index come from the file's
# own header and Cues. ffmpeg is then fed straight from the right byte offset, so a switch costs
# ONE process and ONE Telegram request. Results are cached in MongoDB (collection "media").
# ====================================================================
NICE = shutil.which("nice")
PROBE_SEM = asyncio.Semaphore(1)
EXTRACT_SEM = asyncio.Semaphore(Player.EXTRACT_LIMIT)
REMUX_SEM = asyncio.Semaphore(Player.REMUX_LIMIT)
MEDIA_V = 1
EBML_MAGIC = b"\x1a\x45\xdf\xa3"
SEGMENT_UNKNOWN = b"\x18\x53\x80\x67\x01\xff\xff\xff\xff\xff\xff\xff"  # Segment header, "size unknown"
(ID_SEGMENT, ID_SEEKHEAD, ID_SEEK, ID_SEEKID, ID_SEEKPOS) = (0x18538067, 0x114D9B74, 0x4DBB, 0x53AB, 0x53AC)
(ID_INFO, ID_TSCALE, ID_DURATION, ID_TRACKS, ID_TRACKENTRY) = (0x1549A966, 0x2AD7B1, 0x4489, 0x1654AE6B, 0xAE)
(ID_TNUM, ID_TTYPE, ID_CODEC, ID_LANG, ID_LANG2, ID_NAME) = (0xD7, 0x83, 0x86, 0x22B59C, 0x22B59D, 0x536E)
(ID_DEFAULT, ID_FORCED, ID_AUDIO, ID_CHANNELS, ID_VIDEO, ID_WIDTH, ID_HEIGHT) = (0x88, 0x55AA, 0xE1, 0x9F, 0xE0, 0xB0, 0xBA)
(ID_CUES, ID_CUEPOINT, ID_CUETIME, ID_CUETRACKPOS, ID_CUETRACK, ID_CUECLUSTERPOS) = (0x1C53BB6B, 0xBB, 0xB3, 0xB7, 0xF7, 0xF1)
ID_CLUSTER = 0x1F43B675
CODEC_PREFIXES = [
    ("V_MPEG4/ISO/AVC", "h264"), ("V_MPEGH/ISO/HEVC", "hevc"), ("V_VP9", "vp9"), ("V_VP8", "vp8"), ("V_AV1", "av1"),
    ("V_MPEG4/ISO/", "mpeg4"), ("A_AAC", "aac"), ("A_AC3", "ac3"), ("A_EAC3", "eac3"), ("A_DTS", "dts"),
    ("A_OPUS", "opus"), ("A_VORBIS", "vorbis"), ("A_TRUEHD", "truehd"), ("A_FLAC", "flac"), ("A_MPEG/L3", "mp3"),
    ("A_MPEG/L2", "mp2"), ("A_PCM", "pcm"), ("S_TEXT/UTF8", "subrip"), ("S_TEXT/ASS", "ass"), ("S_TEXT/SSA", "ass"),
    ("S_ASS", "ass"), ("S_SSA", "ass"), ("S_TEXT/WEBVTT", "webvtt"), ("S_HDMV/PGS", "hdmv_pgs_subtitle"),
    ("S_VOBSUB", "dvd_subtitle"), ("S_DVBSUB", "dvb_subtitle"),
]
TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text", "microdvd", "subviewer", "sami"}
AUDIO_TARGET = {"aac": "aac", "libopus": "opus", "libmp3lame": "mp3"}  # encoder -> codec name it produces

_media: dict = {}         # db_id -> analysis (persisted in MongoDB)
_subs: dict = {}          # db_id -> {track n: cues}
_subs_state: dict = {}    # db_id -> {"state": working|ready|none|skipped|failed, "progress": 0..1}
_tasks: dict = {}         # db_id -> running analysis task
_bg: set = set()          # keeps background tasks alive
_failed: dict = {}        # db_id -> time of last failed analysis


def _remember(cache: dict, key, value, cap: int):
    if len(cache) >= cap:
        cache.pop(next(iter(cache)))
    cache[key] = value


def js_json(obj) -> str:
    """JSON that is safe to place inside a <script> block."""
    return json.dumps(obj, separators=(",", ":")).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


# ---------------------------------------------------------------- EBML / Matroska parsing
def _vint(buf, pos, marker=False):
    b0 = buf[pos]
    if not b0:
        raise ValueError("bad vint")
    length = 9 - b0.bit_length()
    if pos + length > len(buf):
        raise IndexError("truncated")
    val = b0 if marker else b0 & ((1 << (8 - length)) - 1)
    for i in range(1, length):
        val = (val << 8) | buf[pos + i]
    return val, length


def _children(buf, start, end):
    """Yields (id, element_start, data_start, data_end, size_unknown) for the elements in buf[start:end]."""
    pos = start
    while pos < end:
        try:
            eid, il = _vint(buf, pos, True)
            size, sl = _vint(buf, pos + il)
        except (IndexError, ValueError):
            return
        ds = pos + il + sl
        unknown = size == (1 << (7 * sl)) - 1
        de = end if unknown else ds + size
        yield eid, pos, ds, de, unknown
        pos = de


def _uint(buf, ds, de):
    return int.from_bytes(buf[ds:de], "big")


def _float(buf, ds, de):
    b = bytes(buf[ds:de])
    return struct.unpack(">f", b)[0] if len(b) == 4 else struct.unpack(">d", b)[0] if len(b) == 8 else 0.0


def _str(buf, ds, de):
    return bytes(buf[ds:de]).split(b"\0")[0].decode("utf-8", "replace")


def _payload_start(raw):
    _, il = _vint(raw, 0, True)
    _, sl = _vint(raw, il)
    return il + sl


def mkv_head(buf):
    """Reads the start of an MKV/WebM file. Returns None if it is not Matroska, otherwise a dict:
    ebml (header bytes), seg_start, seek {element id: position in segment}, info / tracks (raw
    element bytes when they lie inside buf, else None)."""
    if buf[:4] != EBML_MAGIC:
        return None
    _, il = _vint(buf, 0, True)
    size, sl = _vint(buf, il)
    ebml_end = il + sl + size
    eid, il = _vint(buf, ebml_end, True)
    if eid != ID_SEGMENT:
        return None
    _, sl = _vint(buf, ebml_end + il)
    seg_start = ebml_end + il + sl
    out = {"ebml": bytes(buf[:ebml_end]), "seg_start": seg_start, "seek": {}, "info": None, "tracks": None}
    for eid, pos, ds, de, _unk in _children(buf, seg_start, len(buf)):
        if eid == ID_CLUSTER or de > len(buf):
            break
        if eid == ID_SEEKHEAD:
            for sid_el, _p, s_ds, s_de, _u in _children(buf, ds, de):
                if sid_el != ID_SEEK:
                    continue
                sid = spos = None
                for k, _p2, k_ds, k_de, _u2 in _children(buf, s_ds, s_de):
                    if k == ID_SEEKID:
                        sid = _uint(buf, k_ds, k_de)
                    elif k == ID_SEEKPOS:
                        spos = _uint(buf, k_ds, k_de)
                if sid is not None and spos is not None:
                    out["seek"][sid] = spos
        elif eid == ID_INFO:
            out["info"] = bytes(buf[pos:de])
        elif eid == ID_TRACKS:
            out["tracks"] = bytes(buf[pos:de])
    return out


def parse_mkv_info(raw):
    """-> (timecode scale in ns, duration in seconds)"""
    scale, dur = 1000000, 0.0
    for eid, _p, ds, de, _u in _children(raw, _payload_start(raw), len(raw)):
        if eid == ID_TSCALE:
            scale = _uint(raw, ds, de)
        elif eid == ID_DURATION:
            dur = _float(raw, ds, de)
    return scale, dur * scale / 1e9


def _codec_name(codec_id: str) -> str:
    for prefix, name in CODEC_PREFIXES:
        if codec_id.startswith(prefix):
            return name
    return codec_id.lower()


def parse_mkv_tracks(raw):
    """-> {"video": {...}|None, "audio": [...], "subs": [...]} in file order (= ffmpeg's 0:a:n / 0:s:n)."""
    video, audio, subs = None, [], []
    for eid, _p, ds, de, _u in _children(raw, _payload_start(raw), len(raw)):
        if eid != ID_TRACKENTRY:
            continue
        t = {"num": None, "type": 0, "codec": "", "lang": "", "lang2": "", "title": "", "default": True,
             "forced": False, "channels": 1, "w": 0, "h": 0}
        for k, _p2, k_ds, k_de, _u2 in _children(raw, ds, de):
            if k == ID_TNUM:
                t["num"] = _uint(raw, k_ds, k_de)
            elif k == ID_TTYPE:
                t["type"] = _uint(raw, k_ds, k_de)
            elif k == ID_CODEC:
                t["codec"] = _codec_name(_str(raw, k_ds, k_de))
            elif k == ID_LANG:
                t["lang"] = _str(raw, k_ds, k_de)
            elif k == ID_LANG2:
                t["lang2"] = _str(raw, k_ds, k_de)
            elif k == ID_NAME:
                t["title"] = _str(raw, k_ds, k_de)
            elif k == ID_DEFAULT:
                t["default"] = bool(_uint(raw, k_ds, k_de))
            elif k == ID_FORCED:
                t["forced"] = bool(_uint(raw, k_ds, k_de))
            elif k == ID_AUDIO:
                for a, _p3, a_ds, a_de, _u3 in _children(raw, k_ds, k_de):
                    if a == ID_CHANNELS:
                        t["channels"] = _uint(raw, a_ds, a_de)
            elif k == ID_VIDEO:
                for a, _p3, a_ds, a_de, _u3 in _children(raw, k_ds, k_de):
                    if a == ID_WIDTH:
                        t["w"] = _uint(raw, a_ds, a_de)
                    elif a == ID_HEIGHT:
                        t["h"] = _uint(raw, a_ds, a_de)
        lang = t["lang"] if t["lang"] and t["lang"] != "und" else t["lang2"]
        item = {"lang": lang or "", "title": t["title"], "codec": t["codec"], "default": t["default"]}
        if t["type"] == 1 and video is None:
            video = {"num": t["num"], "codec": t["codec"], "w": t["w"], "h": t["h"]}
        elif t["type"] == 2:
            audio.append({**item, "n": len(audio), "channels": t["channels"]})
        elif t["type"] == 17:
            subs.append({**item, "n": len(subs), "forced": t["forced"], "text": t["codec"] in TEXT_SUB_CODECS})
    return {"video": video, "audio": audio, "subs": subs}


def parse_mkv_cues(raw, seg_start, scale, video_num):
    """Keyframe index -> [(seconds, absolute byte position of its cluster)], one entry per cluster."""
    pts = []
    for eid, _p, ds, de, _u in _children(raw, _payload_start(raw), len(raw)):
        if eid != ID_CUEPOINT or de > len(raw):
            continue
        t, cand = None, []
        for k, _p2, k_ds, k_de, _u2 in _children(raw, ds, de):
            if k == ID_CUETIME:
                t = _uint(raw, k_ds, k_de)
            elif k == ID_CUETRACKPOS:
                trk = pos = None
                for a, _p3, a_ds, a_de, _u3 in _children(raw, k_ds, k_de):
                    if a == ID_CUETRACK:
                        trk = _uint(raw, a_ds, a_de)
                    elif a == ID_CUECLUSTERPOS:
                        pos = _uint(raw, a_ds, a_de)
                if pos is not None and (video_num is None or trk == video_num):
                    cand.append(pos)
        if t is not None and cand:
            pts.append((t * scale / 1e9, seg_start + cand[0]))
    pts.sort()
    out, seen = [], set()
    for t, p in pts:  # first (earliest) keyframe of each cluster: that is where a cut stream begins
        if p not in seen:
            seen.add(p)
            out.append([round(t, 3), p])
    return out


async def read_range(fid: FileId, start: int, length: int, prefetch=None) -> bytes:
    end = min(start + length, fid.file_size) - 1
    if end < start:
        return b""
    offset = start - start % CHUNK
    parts = end // CHUNK - offset // CHUNK + 1
    out = []
    async with aclosing(stream_chunks(fid, offset, start - offset, end % CHUNK + 1, parts, prefetch)) as it:
        async for c in it:
            out.append(c)
    return b"".join(out)


async def read_element(fid: FileId, pos: int, cap: int = 16 * 1024 * 1024) -> bytes:
    buf = await read_range(fid, pos, CHUNK)
    total = _payload_start(buf) + _vint(buf, _vint(buf, 0, True)[1])[0]
    if total > cap:
        raise ValueError("element too large")
    return buf[:total] if total <= len(buf) else await read_range(fid, pos, total)


async def analyze_mkv(fid: FileId, head: bytes) -> dict:
    h = mkv_head(head)
    if not h:
        raise ValueError("not matroska")
    seg = h["seg_start"]
    for key, eid in (("info", ID_INFO), ("tracks", ID_TRACKS)):
        if h[key] is None:
            rel = h["seek"].get(eid)
            if rel is None:
                raise ValueError(f"no {key} element")
            h[key] = await read_element(fid, seg + rel)
    scale, duration = parse_mkv_info(h["info"])
    tr = parse_mkv_tracks(h["tracks"])
    cues = []
    rel = h["seek"].get(ID_CUES)
    if rel is not None:
        try:
            cues = parse_mkv_cues(await read_element(fid, seg + rel), seg, scale, (tr["video"] or {}).get("num"))
        except Exception as e:
            logging.warning(f"cues unreadable ({e!r}); audio switching will use the slower path")
    v = tr["video"] or {}
    return {
        "v": MEDIA_V, "kind": "mkv", "duration": duration, "vcodec": v.get("codec", ""),
        "vw": v.get("w", 0), "vh": v.get("h", 0), "audio": tr["audio"], "subs": tr["subs"], "cues": cues,
        "hdr": base64.b64encode(h["ebml"] + SEGMENT_UNKNOWN + h["info"] + h["tracks"]).decode(),
    }


# ---------------------------------------------------------------- ffmpeg helpers
def local_url(db_id: str) -> str:
    return f"http://127.0.0.1:{Server.PORT}/dl/{db_id}"


def low_priority(cmd: list, nice: int = 19) -> list:
    return [NICE, "-n", str(nice)] + cmd if NICE else cmd


def probe_cmd(url):
    return ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_format", url]


def parse_ffprobe(data: dict) -> dict:
    audio, subs, vcodec, vw, vh = [], [], "", 0, 0
    for s in data.get("streams", []):
        tags = {k.lower(): v for k, v in (s.get("tags") or {}).items()}
        disp = s.get("disposition") or {}
        kind = s.get("codec_type")
        item = {"lang": tags.get("language", "") if tags.get("language") != "und" else "",
                "title": tags.get("title", ""), "codec": s.get("codec_name", ""), "default": bool(disp.get("default"))}
        if kind == "audio":
            audio.append({**item, "n": len(audio), "channels": s.get("channels", 0)})
        elif kind == "subtitle":
            subs.append({**item, "n": len(subs), "forced": bool(disp.get("forced")),
                         "text": s.get("codec_name") in TEXT_SUB_CODECS})
        elif kind == "video" and not vcodec and not disp.get("attached_pic"):
            vcodec, vw, vh = s.get("codec_name", ""), s.get("width", 0), s.get("height", 0)
    try:
        duration = float((data.get("format") or {}).get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0
    return {"duration": duration, "vcodec": vcodec, "vw": vw, "vh": vh, "audio": audio, "subs": subs}


def seekpoint_cmd(url, t: float):
    return ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=pts_time,flags",
            "-read_intervals", f"{t:.3f}%+#1", "-print_format", "json", url]


def parse_seekpoint(data: dict, fallback: float) -> float:
    try:
        return min(float(data["packets"][0]["pts_time"]), fallback)
    except (KeyError, IndexError, TypeError, ValueError):
        return fallback


def remux_cmd(media: dict, track: dict, a: int, source: str, start: float = 0.0) -> list:
    """source == "pipe" -> matroska read from stdin (already starts at the right cluster);
    otherwise `source` is a URL and the file is seeked to `start`."""
    if source == "pipe":
        pre = ["-probesize", "2M", "-analyzeduration", "2M", "-f", "matroska", "-i", "pipe:0"]
    else:  # +0.01: land ON the keyframe /seekpoint reported, not the one before it
        pre = ["-nostdin", "-probesize", "5M", "-analyzeduration", "5M", "-ss", f"{start + 0.01:.3f}", "-i", source]
    target = AUDIO_TARGET.get(Player.AUDIO_CODEC, Player.AUDIO_CODEC)
    if track.get("codec") == target:
        audio = ["-c:a", "copy"]  # e.g. AAC -> AAC: no re-encode, almost no CPU
    else:
        audio = ["-c:a", Player.AUDIO_CODEC, "-b:a", Player.AUDIO_BITRATE] + (["-ac", "2"] if track.get("channels", 2) > 2 else [])
    video = ["-c:v", "copy"] + (["-tag:v", "hvc1"] if media.get("vcodec") == "hevc" else [])
    return (["ffmpeg", "-v", "error"] + pre + ["-map", "0:V:0", "-map", f"0:a:{a}"] + video + audio
            + ["-sn", "-dn", "-map_chapters", "-1", "-flush_packets", "1", "-f", "mp4",
               "-movflags", "frag_keyframe+empty_moov+default_base_moof", "-frag_duration", "1000000", "pipe:1"])


async def run_capture(cmd: list, timeout: float, nice: int = 10):
    proc = await asyncio.create_subprocess_exec(
        *low_priority(cmd, nice), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
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


async def pump_stdin(proc, source, background: bool = False, on_bytes=None):
    """Copies an async byte source into a process' stdin (respecting its speed)."""
    try:
        async with aclosing(source) as it:
            async for chunk in it:
                if background:
                    await yield_to_streams()
                proc.stdin.write(chunk)
                await proc.stdin.drain()
                if on_bytes:
                    on_bytes(len(chunk))
    except (BrokenPipeError, ConnectionResetError):
        pass  # ffmpeg exited / was killed
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass


async def yield_to_streams():
    """Background work steps aside for real downloads and streams."""
    n = active_streams
    while n > Streaming.BG_PAUSE_AT:
        await asyncio.sleep(1)
        n = active_streams
    if n:
        await asyncio.sleep(0.01 * n)


async def cut_source(fid: FileId, start: int, header: bytes = b"", prefetch=None):
    """Optional header, then the file from byte `start` to the end."""
    if header:
        yield header
    size = fid.file_size
    offset = start - start % CHUNK
    parts = (size - 1) // CHUNK - offset // CHUNK + 1
    async with aclosing(stream_chunks(fid, offset, start - offset, (size - 1) % CHUNK + 1, parts, prefetch)) as it:
        async for c in it:
            yield c


# ---------------------------------------------------------------- persistence + analysis
def _col():
    return db.file.database["media"]


async def _db_get(key: str):
    try:
        doc = await asyncio.wait_for(_col().find_one({"_id": key}), 5)
        if doc:
            doc.pop("_id", None)
        return doc
    except Exception as e:
        logging.warning(f"media cache read failed: {e!r}")
        return None


async def _db_put(key: str, doc: dict):
    try:
        await asyncio.wait_for(_col().replace_one({"_id": key}, {**doc, "_id": key}, upsert=True), 10)
    except Exception as e:
        logging.warning(f"media cache write failed: {e!r}")


async def analyze_ffprobe(db_id: str) -> dict:
    async with PROBE_SEM:
        rc, out = await run_capture(probe_cmd(local_url(db_id)), 90)
    if rc != 0:
        raise RuntimeError(f"ffprobe exit {rc}")
    return {"v": MEDIA_V, "kind": "other", **parse_ffprobe(json.loads(out)), "cues": [], "hdr": ""}


async def _analyze(db_id: str) -> dict:
    fid = await get_file_id(db_id)
    media = None
    try:
        head = await read_range(fid, 0, CHUNK)
        if head[:4] == EBML_MAGIC:
            media = await analyze_mkv(fid, head)
    except Exception as e:
        logging.warning(f"mkv analysis of {db_id} failed ({e!r}); falling back to ffprobe")
    if media is None:
        media = await analyze_ffprobe(db_id)
    media["size"] = fid.file_size
    _remember(_media, db_id, media, 300)
    await _db_put(db_id, media)
    return media


async def get_media(db_id: str, cached_only: bool = False):
    m = _media.get(db_id)
    if m or cached_only and db_id in _media:
        return m
    doc = await _db_get(db_id)
    if doc and doc.get("v") == MEDIA_V:
        _remember(_media, db_id, doc, 300)
        return doc
    if cached_only:
        return None
    if time.time() - _failed.get(db_id, 0) < 30:
        raise RuntimeError("analysis failed recently")
    task = _tasks.get(db_id)
    if task is None:
        task = _tasks[db_id] = asyncio.create_task(_analyze(db_id))

        def done(t, key=db_id):
            _tasks.pop(key, None)
            if t.cancelled() or t.exception():
                _failed[key] = time.time()
        task.add_done_callback(done)
    return await asyncio.shield(task)


def public_info(db_id: str, media: dict) -> dict:
    st = _subs_state.get(db_id) or {"state": "idle"}
    cues = media.get("cues") or []
    return {
        "status": "ready", "duration": media.get("duration", 0), "vcodec": media.get("vcodec", ""),
        "audio": media["audio"], "subs": media["subs"], "keyframes": [c[0] for c in cues] or None,
        "subs_state": st["state"], "subs_progress": st.get("progress"),
    }


# ---------------------------------------------------------------- subtitles
_TS = re.compile(r"(?:(\d+):)?(\d+):(\d+)[.,](\d+)")
_ANY_TAG = re.compile(r"</?[^>]*>")
_KEEP_TAG = re.compile(r"(</?[ibu]>)", re.I)


def clean_cue(text: str) -> str:
    """Subtitle text -> safe HTML: only <i> <b> <u> and line breaks survive; everything else is escaped."""
    text = re.sub(r"\{\\[^}]*\}", "", text)  # leftover ASS {\an8} style overrides
    parts = []
    for p in _KEEP_TAG.split(text):
        parts.append(p.lower() if _KEEP_TAG.fullmatch(p) else html.escape(_ANY_TAG.sub("", p), quote=False))
    return "".join(parts).strip().replace("\n", "<br>")


def _ts(s: str):
    m = _TS.match(s.strip())
    if not m:
        return None
    h, mi, se, ms = m.groups()
    return int(h or 0) * 3600 + int(mi) * 60 + int(se) + int(ms.ljust(3, "0")[:3]) / 1000


def parse_vtt(text: str) -> list:
    out, last = [], None
    for block in re.split(r"\n{2,}", text.replace("\r", "").strip()):
        lines = block.split("\n")
        i = next((k for k, ln in enumerate(lines) if "-->" in ln), -1)
        if i < 0:
            continue
        a, _, b = lines[i].partition("-->")
        s, e = _ts(a), _ts(b.strip().split(" ")[0] if b.strip() else "")
        body = clean_cue("\n".join(lines[i + 1:]))
        if s is None or e is None or not body:
            continue
        key = (round(s, 2), round(e, 2), body)
        if key != last:  # ASS layers often repeat the same line
            out.append([key[0], key[1], body])
            last = key
    out.sort(key=lambda c: c[0])
    return out[:20000]


async def extract_subs(db_id: str, media: dict, st: dict) -> dict:
    """ONE pass over the file extracts every text subtitle track (they are interleaved with the video,
    so the whole file has to be read once; that is why it is cached and runs in the background)."""
    tracks = [s for s in media["subs"] if s["text"]]
    fid = await get_file_id(db_id)
    mkv = media["kind"] == "mkv"
    tmp = tempfile.mkdtemp(prefix="subs-")
    proc = feeder = err_task = None
    try:
        cmd = ["ffmpeg", "-v", "error"] + (["-f", "matroska", "-i", "pipe:0"] if mkv else ["-nostdin", "-i", local_url(db_id)])
        for t in tracks:
            cmd += ["-map", f"0:s:{t['n']}", "-c:s", "webvtt", "-f", "webvtt", os.path.join(tmp, f"{t['n']}.vtt")]
        proc = await asyncio.create_subprocess_exec(
            *low_priority(cmd, 19), stdin=asyncio.subprocess.PIPE if mkv else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
        )
        err_task = asyncio.create_task(proc.stderr.read())
        if mkv:
            fed = [0]

            def progress(n):
                fed[0] += n
                st["progress"] = round(min(fed[0] / max(fid.file_size, 1), 0.99), 4)
            feeder = asyncio.create_task(pump_stdin(
                proc, cut_source(fid, 0, prefetch=Streaming.BG_PREFETCH), background=True, on_bytes=progress))
            await feeder
        await proc.wait()
        result = {}
        for t in tracks:
            path = os.path.join(tmp, f"{t['n']}.vtt")
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, encoding="utf-8", errors="replace") as f:
                    result[str(t["n"])] = parse_vtt(f.read())
        if not result:
            tail = (await err_task).decode("utf-8", "replace")[-300:]
            raise RuntimeError(f"ffmpeg produced no subtitles (exit {proc.returncode}): {tail}")
        return result
    finally:
        for t in (feeder, err_task):
            if t and not t.done():
                t.cancel()
        if proc and proc.returncode is None:
            proc.kill()
            asyncio.create_task(proc.wait())
        shutil.rmtree(tmp, ignore_errors=True)


async def _run_subs(db_id: str, media: dict):
    st = _subs_state[db_id]
    try:
        async with EXTRACT_SEM:
            result = await extract_subs(db_id, media, st)
        _remember(_subs, db_id, result, 20)
        await _db_put(db_id + ":subs", {"tracks": result})
        st.update(state="ready", progress=1.0)
        logging.info(f"[subs] {db_id}: {len(result)} track(s) ready")
    except asyncio.CancelledError:
        _subs_state.pop(db_id, None)
        raise
    except Exception as e:
        logging.warning(f"[subs] {db_id} failed: {e!r}")
        st.update(state="failed", at=time.time())


async def kick_subs(db_id: str, media: dict):
    """Starts (once) the background subtitle extraction for a file; safe to call on every /info."""
    st = _subs_state.get(db_id)
    if st and not (st["state"] == "failed" and time.time() - st.get("at", 0) > 600):
        return
    if not any(s["text"] for s in media["subs"]):
        _subs_state[db_id] = {"state": "none"}
        return
    st = _subs_state[db_id] = {"state": "working", "progress": None if media["kind"] != "mkv" else 0.0}
    doc = await _db_get(db_id + ":subs")
    if doc and doc.get("tracks"):
        _remember(_subs, db_id, doc["tracks"], 20)
        st.update(state="ready", progress=1.0)
    elif not Player.SUBS_AUTO or media.get("size", 0) > Player.SUBS_MAX_GB * 1024 ** 3:
        st.update(state="skipped")
    else:
        task = asyncio.create_task(_run_subs(db_id, media))
        _bg.add(task)
        task.add_done_callback(_bg.discard)


# ---------------------------------------------------------------- routes
async def _require_file(db_id: str):
    try:
        return await get_file_id(db_id)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)


def _num(request, key, cast, default):
    try:
        return cast(request.query.get(key, default))
    except ValueError:
        raise web.HTTPBadRequest(text=f"bad {key}")


@routes.get("/info/{path}")
async def info_route(request: web.Request):
    db_id = request.match_info["path"]
    await _require_file(db_id)
    try:
        media = await asyncio.wait_for(get_media(db_id), 25)
    except asyncio.TimeoutError:
        return web.json_response({"status": "working"})
    except Exception as e:
        logging.warning(f"analysis of {db_id} failed: {e!r}")
        return web.json_response({"status": "error"})
    await kick_subs(db_id, media)
    return web.json_response(public_info(db_id, media), headers={"Cache-Control": "no-store"})


@routes.get("/subs/{path}")
async def subs_route(request: web.Request):
    db_id = request.match_info["path"]
    await _require_file(db_id)
    tracks = _subs.get(db_id)
    if tracks is None:
        doc = await _db_get(db_id + ":subs")
        if doc and doc.get("tracks"):
            tracks = doc["tracks"]
            _remember(_subs, db_id, tracks, 20)
    if tracks is None:
        return web.json_response({"state": (_subs_state.get(db_id) or {}).get("state", "idle")}, status=202)
    resp = web.json_response({"tracks": tracks}, headers={"Cache-Control": "public, max-age=3600"})
    resp.enable_compression()
    return resp


@routes.get("/seekpoint/{path}")
async def seekpoint_route(request: web.Request):
    """Only used for files without a keyframe index (non-MKV)."""
    db_id = request.match_info["path"]
    t = max(0.0, _num(request, "t", float, "0"))
    await _require_file(db_id)
    start = t
    if t > 0:
        try:
            async with PROBE_SEM:
                rc, out = await run_capture(seekpoint_cmd(local_url(db_id), t), 40)
            if rc == 0 and out:
                start = parse_seekpoint(json.loads(out), t)
        except Exception as e:
            logging.warning(f"seekpoint failed for {db_id}: {e!r}")
    return web.json_response({"start": round(start, 3)})


@routes.get("/audio/{path}")
async def audio_route(request: web.Request):
    global active_streams
    db_id = request.match_info["path"]
    a = _num(request, "a", int, "0")
    t = max(0.0, _num(request, "t", float, "0"))
    fid = await _require_file(db_id)
    try:
        media = await get_media(db_id)
    except Exception:
        raise web.HTTPServiceUnavailable(text="Track list not ready", headers={"Retry-After": "3"})
    if not 0 <= a < len(media["audio"]):
        raise web.HTTPBadRequest(text="no such audio track")
    track = media["audio"][a]
    try:
        await asyncio.wait_for(REMUX_SEM.acquire(), timeout=Streaming.QUEUE_WAIT)
    except asyncio.TimeoutError:
        raise web.HTTPServiceUnavailable(text="Audio switching is busy, retry shortly", headers={"Retry-After": "5"})

    active_streams += 1
    proc = feeder = err_task = None
    try:
        cues = media.get("cues") or []
        source = None
        if media["kind"] == "mkv" and cues and media.get("hdr"):
            i = max(0, bisect.bisect_right([c[0] for c in cues], t + 0.05) - 1)
            cmd = remux_cmd(media, track, a, "pipe")
            source = cut_source(fid, cues[i][1], base64.b64decode(media["hdr"]))
        else:
            cmd = remux_cmd(media, track, a, local_url(db_id), t)
        proc = await asyncio.create_subprocess_exec(
            *low_priority(cmd, 10),
            stdin=asyncio.subprocess.PIPE if source else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        err_task = asyncio.create_task(proc.stderr.read())
        if source:
            feeder = asyncio.create_task(pump_stdin(proc, source))
        try:
            first = await asyncio.wait_for(proc.stdout.read(65536), 90)
        except asyncio.TimeoutError:
            first = b""
        if not first:
            await asyncio.sleep(0.2)
            tail = (await err_task).decode("utf-8", "replace")[-300:] if err_task.done() else ""
            logging.warning(f"[audio] {db_id} track {a} failed: {tail}")
            raise web.HTTPBadGateway(text="Could not switch audio for this file")
        response = web.StreamResponse(
            status=200, headers={"Content-Type": "video/mp4", "Cache-Control": "no-store", "Accept-Ranges": "none"}
        )
        await response.prepare(request)
        try:
            await response.write(first)
            while True:
                chunk = await proc.stdout.read(256 * 1024)
                if not chunk:
                    break
                await response.write(chunk)
            await response.write_eof()
        except (ConnectionResetError, BrokenPipeError):
            pass  # viewer went away or switched again
        return response
    finally:
        active_streams -= 1
        REMUX_SEM.release()
        for tk in (feeder, err_task):
            if tk and not tk.done():
                tk.cancel()
        if proc and proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            asyncio.create_task(proc.wait())


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
