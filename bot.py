"""Telegram side: config, database, and the (few) bot commands.

Features: send a file -> get stream / download / mediainfo links.
Owner commands: /status /ban /unban /broadcast /del
Settings live in config.py.
"""
import asyncio
import logging
import re
import time
from datetime import datetime

import motor.motor_asyncio
from bson.errors import InvalidId
from bson.objectid import ObjectId
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, InputUserDeactivated, PeerIdInvalid, UserIsBlocked
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Telegram, base_url

StartTime = time.time()


class FIleNotFound(Exception):
    message = "File not found"


# ====================================================================
# HELPERS
# ====================================================================
def humanbytes(size):
    if not size:
        return ""
    power, n = 1024, 0
    units = ["", "Ki", "Mi", "Gi", "Ti"]
    while size > power and n < 4:
        size /= power
        n += 1
    return f"{round(size, 2)} {units[n]}B"


def uptime(seconds: float) -> str:
    d, r = divmod(int(seconds), 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    return f"{d}d {h}h {m}m {s}s"


def clean_filename(name: str) -> str:
    name = re.sub(r"@[a-zA-Z0-9_]+", "", name)
    name = re.sub(r"[^a-zA-Z0-9\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


START_TEXT = """<b>🔆 [ DOWNLOAD / STREAM ] 🔆
⌬──━━━━━━━━━━━━━━──⌬
♻ Send me any file and get a fast Download / Stream link!</b>"""

STREAM_TEXT = """<b>[ DOWNLOAD / STREAM ]
⌬──━━━━━━━━━━──⌬
🔆 [ {} ] {}

♻️ Dᴏᴡɴʟᴏᴀᴅ Link ➜ {}

🌟 Sᴛʀᴇᴀᴍ Link ➜ {}</b>"""

STREAM_TEXT_X = """<b>[ DOWNLOAD / STREAM ]
⌬──━━━━━━━━━━──⌬
🔆 [ {} ] {}

♻️ Dᴏᴡɴʟᴏᴀᴅ Link ➜ {}
📒 MediaInfo:- Available via Bot / Instant View</b>"""

BAN_TEXT = "__Sᴏʀʀʏ Sɪʀ, Yᴏᴜ ᴀʀᴇ Bᴀɴɴᴇᴅ ᴛᴏ ᴜsᴇ ᴍᴇ.__\n\n**[Cᴏɴᴛᴀᴄᴛ Dᴇᴠᴇʟᴏᴘᴇʀ](tg://user?id={}) Tʜᴇʏ Wɪʟʟ Hᴇʟᴘ Yᴏᴜ**"

START_BUTTONS = InlineKeyboardMarkup(
    [[
        InlineKeyboardButton("📢 Updates", url=Telegram.UPDATES_CHANNEL),
        InlineKeyboardButton("👤 Admin", url="https://telegram.me/mrkrazybot"),
    ]]
)


def make_links(db_id, name: str, size, mime: str):
    """Returns (reply_markup, text) with stream / download / mediainfo links."""
    base = base_url()
    page, dl, info = f"{base}watch/{db_id}", f"{base}dl/{db_id}", f"{base}mediainfo/{db_id}"
    name, size = clean_filename(name), humanbytes(size)
    if "video" in mime:
        text = STREAM_TEXT.format(size, name, dl, page)
        rows = [[InlineKeyboardButton("sᴛʀᴇᴀᴍ", url=page), InlineKeyboardButton("ᴅᴏᴡɴʟᴏᴀᴅ", url=dl)]]
    else:
        text = STREAM_TEXT_X.format(size, name, dl)
        rows = [[InlineKeyboardButton("ᴅᴏᴡɴʟᴏᴀᴅ", url=dl)]]
    rows.append([InlineKeyboardButton("📄 ᴍᴇᴅɪᴀɪɴғᴏ ʀᴇᴘᴏʀᴛ", url=info)])
    return InlineKeyboardMarkup(rows), text


# ====================================================================
# DATABASE (MongoDB)
# ====================================================================
class Database:
    def __init__(self, uri, name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.col = self._client[name].users
        self.black = self._client[name].blacklist
        self.file = self._client[name].file
        self.banned: set = set()  # kept in memory: no DB hit per message
        self.known: set = set()   # users already stored this run

    async def init(self):
        """Create indexes and load the ban list."""
        try:
            await self.file.create_index([("user_id", 1), ("file_unique_id", 1)])
            await self.col.create_index("id")
            await self.black.create_index("id")
        except Exception as e:
            logging.warning(f"index creation failed: {e}")
        self.banned = {d["id"] async for d in self.black.find({}, {"id": 1})}

    # users
    async def touch_user(self, uid):
        if uid not in self.known:
            await self.col.update_one(
                {"id": uid}, {"$setOnInsert": {"id": uid, "join_date": time.time()}}, upsert=True
            )
            self.known.add(uid)

    async def delete_user(self, uid):
        self.known.discard(uid)
        await self.col.delete_many({"id": int(uid)})

    async def total_users(self):
        return await self.col.count_documents({})

    # bans
    async def ban(self, uid):
        self.banned.add(uid)
        await self.black.update_one({"id": uid}, {"$set": {"id": uid, "ban_date": time.time()}}, upsert=True)

    async def unban(self, uid):
        self.banned.discard(uid)
        await self.black.delete_many({"id": uid})

    # files
    async def add_file(self, info) -> ObjectId:
        """Stores a file once per (user, unique id); returns its id."""
        old = await self.file.find_one(
            {"user_id": info["user_id"], "file_unique_id": info["file_unique_id"]}, {"_id": 1}
        )
        if old:
            return old["_id"]
        info["time"] = time.time()
        return (await self.file.insert_one(info)).inserted_id

    async def get_file(self, _id):
        try:
            doc = await self.file.find_one({"_id": ObjectId(_id)})
        except InvalidId:
            raise FIleNotFound
        if not doc:
            raise FIleNotFound
        return doc

    async def set_log_msg(self, _id, msg_id):
        await self.file.update_one({"_id": ObjectId(_id)}, {"$set": {"log_msg_id": msg_id}})

    async def delete_file(self, _id):
        await self.file.delete_one({"_id": ObjectId(_id)})

    async def total_files(self):
        return await self.file.count_documents({})


db = Database(Telegram.DATABASE_URL, Telegram.DB_NAME)

FileStream = Client(
    name="FileStream",
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    workdir=".",
    bot_token=Telegram.BOT_TOKEN,
    sleep_threshold=Telegram.SLEEP_THRESHOLD,
    workers=Telegram.WORKERS,
)


# ====================================================================
# USER HANDLERS
# ====================================================================
async def allowed(message) -> bool:
    """Ban check + remember the user (for broadcast)."""
    uid = message.from_user.id
    if uid in db.banned and uid != Telegram.OWNER_ID:
        await message.reply_text(BAN_TEXT.format(Telegram.OWNER_ID), parse_mode=ParseMode.MARKDOWN)
        return False
    await db.touch_user(uid)
    return True


@FileStream.on_message(filters.command("start") & filters.private)
async def start(bot, message):
    if await allowed(message):
        await message.reply_text(
            START_TEXT, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
            reply_markup=START_BUTTONS,
        )


@FileStream.on_message(filters.private & (filters.document | filters.video), group=4)
async def on_file(bot, message):
    if not await allowed(message):
        return
    try:
        media = message.video or message.document
        name = media.file_name or f"file-{datetime.now():%Y-%m-%d_%H-%M-%S}"
        mime = media.mime_type or ("video/mp4" if message.video else "application/octet-stream")
        info = {
            "user_id": message.from_user.id,
            "file_id": media.file_id,
            "file_unique_id": media.file_unique_id,
            "file_name": name,
            "file_size": media.file_size or 0,
            "mime_type": mime,
        }
        db_id = await db.add_file(info)
        doc = await db.get_file(db_id)

        # Copy once into the log channel; streaming reads the fresh file id from that message.
        if not doc.get("log_msg_id") and not doc.get("file_ids"):
            log_msg = await bot.send_cached_media(
                Telegram.FLOG_CHANNEL,
                media.file_id,
                caption=f"{name}\n\nRequested By : [{message.from_user.first_name}]"
                f"(tg://user?id={message.from_user.id}) [`{message.from_user.id}`]",
                parse_mode=ParseMode.MARKDOWN,
            )
            await db.set_log_msg(db_id, log_msg.id)

        markup, text = make_links(db_id, name, info["file_size"], mime)
        await message.reply_text(
            text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
            reply_markup=markup, quote=True,
        )
    except Exception as e:
        logging.exception("file handler failed")
        await message.reply_text(f"Something went wrong: {e}", quote=True)


# ====================================================================
# OWNER COMMANDS
# ====================================================================
owner = filters.private & filters.user(Telegram.OWNER_ID)


def _arg_id(message):
    try:
        return int(message.command[1])
    except (IndexError, ValueError):
        return None


@FileStream.on_message(filters.command("status") & owner)
async def status_cmd(bot, m):
    await m.reply_text(
        f"**Users:** `{await db.total_users()}`\n"
        f"**Banned:** `{len(db.banned)}`\n"
        f"**Files:** `{await db.total_files()}`\n"
        f"**Uptime:** `{uptime(time.time() - StartTime)}`",
        quote=True,
    )


@FileStream.on_message(filters.command("ban") & owner)
async def ban_cmd(bot, m):
    uid = _arg_id(m)
    if uid is None:
        return await m.reply_text("Usage: `/ban user_id`", quote=True)
    await db.ban(uid)
    await db.delete_user(uid)
    await m.reply_text(f"`{uid}` **is Banned**", quote=True)
    try:
        await bot.send_message(uid, "**You are banned from using this bot**")
    except Exception:
        pass


@FileStream.on_message(filters.command("unban") & owner)
async def unban_cmd(bot, m):
    uid = _arg_id(m)
    if uid is None:
        return await m.reply_text("Usage: `/unban user_id`", quote=True)
    await db.unban(uid)
    await m.reply_text(f"`{uid}` **is Unbanned**", quote=True)
    try:
        await bot.send_message(uid, "**You are unbanned, you can use the bot again**")
    except Exception:
        pass


@FileStream.on_message(filters.command("del") & owner)
async def del_cmd(bot, m):
    try:
        doc = await db.get_file(m.command[1])
    except (IndexError, FIleNotFound):
        return await m.reply_text("File not found. Usage: `/del file_id`", quote=True)
    await db.delete_file(doc["_id"])
    await m.reply_text("**File deleted**", quote=True)


async def _copy_to(uid, message):
    """Returns 200 ok, 400 user gone (remove), 500 other failure."""
    try:
        await message.copy(uid)
        return 200
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await _copy_to(uid, message)
    except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
        return 400
    except Exception:
        return 500


@FileStream.on_message(filters.command("broadcast") & owner & filters.reply)
async def broadcast_cmd(bot, m):
    msg = m.reply_to_message
    users = [u["id"] async for u in db.col.find({}, {"id": 1})]
    out = await m.reply_text(f"Broadcast started for {len(users)} users...")
    ok = failed = removed = 0
    t0 = time.time()
    for i in range(0, len(users), 10):  # 10 at a time, well under Telegram's flood limits
        codes = await asyncio.gather(*(_copy_to(uid, msg) for uid in users[i:i + 10]))
        for uid, code in zip(users[i:i + 10], codes):
            if code == 200:
                ok += 1
            else:
                failed += 1
                if code == 400:
                    removed += 1
                    await db.delete_user(uid)
        if (i // 10) % 10 == 0:
            try:
                await out.edit_text(f"Broadcasting... {ok + failed}/{len(users)}")
            except Exception:
                pass
        await asyncio.sleep(0.4)
    await out.edit_text(
        f"**Broadcast done in {int(time.time() - t0)}s**\n\n"
        f"Total: `{len(users)}`\nSent: `{ok}`\nFailed: `{failed}` (removed `{removed}` dead users)"
    )
