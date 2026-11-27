"""All settings. Set these as environment variables (Koyeb -> Service -> Environment) or in a .env file."""
from os import environ as env

from dotenv import load_dotenv

load_dotenv()


class Telegram:
    # --- required ---
    BOT_TOKEN = str(env.get("BOT_TOKEN"))
    DATABASE_URL = str(env.get("DATABASE_URL", ""))          # MongoDB connection string
    API_ID = int(env.get("API_ID", "1736204"))
    API_HASH = str(env.get("API_HASH", "890d40e0f91a4de32dec2965444b2cbe"))
    OWNER_ID = int(env.get("OWNER_ID", "1058015838"))         # can use /ban /broadcast etc.
    FLOG_CHANNEL = int(env.get("FLOG_CHANNEL", "-1001652564383"))  # bot must be admin; files are copied here

    # --- optional ---
    UPDATES_CHANNEL = str(env.get("UPDATES_CHANNEL", "https://telegram.me/+NSpRIGWcoYU2OTk1"))
    DB_NAME = str(env.get("SESSION_NAME", "FileStream"))     # MongoDB database name
    WORKERS = int(env.get("WORKERS", "2"))                   # bot commands run at once (low = more CPU left for streams)
    SLEEP_THRESHOLD = int(env.get("SLEEP_THRESHOLD", "60"))  # auto-wait on Telegram flood waits up to N seconds


class Server:
    FQDN = str(env.get("FQDN", "0.0.0.0"))                   # your public domain, e.g. myapp.koyeb.app
    PORT = int(env.get("PORT", "8080"))                      # must match the port set in Koyeb (8000)
    BIND_ADDRESS = str(env.get("BIND_ADDRESS", "0.0.0.0"))


class Streaming:
    """Downloads/streams come first; everything else (bot commands, mediainfo) gets what is left."""
    MAX_STREAMS = int(env.get("MAX_STREAMS", "40"))          # simultaneous Telegram downloads; extra wait in queue
    QUEUE_WAIT = int(env.get("QUEUE_WAIT", "20"))            # seconds a request waits for a free slot before 503
    PREFETCH = int(env.get("PREFETCH", "4"))                 # max 1 MB chunks fetched ahead per stream (few streams = fast)
    BUFFER_CHUNKS = int(env.get("BUFFER_CHUNKS", "80"))      # total 1 MB chunks buffered across ALL streams (RAM cap ~ this many MB)
    MEDIAINFO_LIMIT = int(env.get("MEDIAINFO_LIMIT", "1"))   # mediainfo reports at once (they are CPU heavy)
    FILE_ID_TTL = int(env.get("FILE_ID_TTL", "1200"))        # seconds a Telegram file id is cached
    BG_PAUSE_AT = int(env.get("BG_PAUSE_AT", "6"))           # background work (subtitle extraction) pauses above this many active streams
    BG_PREFETCH = int(env.get("BG_PREFETCH", "2"))           # chunks fetched ahead by background work


def base_url() -> str:
    """Public base URL, always https and with a trailing slash."""
    host = Server.FQDN.split("://")[-1].rstrip("/")
    return f"https://{host}/"
