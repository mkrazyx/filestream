import asyncio
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone

try:  # optional: much faster event loop for many parallel streams
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from aiohttp import web
from pyrogram import idle

from bot import FileStream, db
from config import Server, Telegram
from server import web_app

logging.basicConfig(
    level=logging.INFO,
    datefmt="%d/%m/%Y %H:%M:%S",
    format="[%(asctime)s] %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

IST = timezone(timedelta(hours=5, minutes=30))
# handler_cancellation: when a viewer disconnects, the Telegram downloads stop at once.
runner = web.AppRunner(web_app, access_log=None, handler_cancellation=True)
loop = asyncio.get_event_loop()


async def start_services():
    # Web server first, so the platform's TCP health check passes even if Telegram is slow.
    await runner.setup()
    await web.TCPSite(runner, Server.BIND_ADDRESS, Server.PORT, backlog=1024).start()
    print(f"Web server listening on {Server.PORT}")

    await FileStream.start()
    me = await FileStream.get_me()
    FileStream.id, FileStream.username, FileStream.fname = me.id, me.username, me.first_name
    await db.init()

    try:
        stamp = datetime.now(tz=IST).strftime("%-d %b %Y | %-I:%M %p")
        await FileStream.send_message(Telegram.OWNER_ID, f"🚀 BOT STARTED\n⏰ {stamp}")
    except Exception:
        print("[Startup] Failed to notify owner.")

    print(f"Service started: @{me.username} (DC {me.dc_id})")
    asyncio.create_task(scheduled_restart())
    await idle()


async def cleanup():
    for step in (runner.cleanup, FileStream.stop):
        try:
            await step()
        except Exception:
            pass


# Full process restart every 12h as a safety net against slow memory growth.
async def scheduled_restart():
    await asyncio.sleep(12 * 60 * 60)
    logging.info("[Restart] 12-hour scheduled restart")
    await cleanup()
    os.execv(sys.executable, [sys.executable, "main.py"])


if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        pass
    except Exception:
        logging.error(traceback.format_exc())
    finally:
        try:
            loop.run_until_complete(cleanup())
        except Exception:
            pass
        loop.stop()
