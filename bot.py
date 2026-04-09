import os
import asyncio
import logging
import time
import tempfile
import shutil
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters
from pyrogram.types import Message, BotCommand
from pyrogram.errors import FloodWait

from youtube_uploader import YouTubeUploader
from database import Database

# ============ CONFIG (Environment Variables) ============
BOT_TOKEN   = os.environ["BOT_TOKEN"]
API_ID      = int(os.environ["API_ID"])
API_HASH    = os.environ["API_HASH"]
MONGO_URI   = os.environ["MONGO_URI"]
OWNER_ID    = int(os.environ["OWNER_ID"])
PORT        = int(os.environ.get("PORT", 8080))
LOG_CHANNEL = os.environ.get("LOG_CHANNEL")
# =======================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    logger.info(f"Health server port {PORT} pe chal raha hai")
    server.serve_forever()


app = Client(
    "yt_uploader_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=8,
    max_concurrent_transmissions=8,
)

youtube = YouTubeUploader()
db = Database(MONGO_URI)

SUPPORTED_MIME = [
    "video/mp4", "video/x-matroska", "video/webm",
    "video/avi", "video/quicktime", "video/x-msvideo",
    "video/mpeg", "video/3gpp"
]
SUPPORTED_EXT = [".mp4", ".mkv", ".webm", ".avi", ".mov", ".mpeg", ".3gp"]

pending_add = {}


def human_size(num):
    for unit in ["B", "KB", "MB", "GB"]:
        if num < 1024:
            return f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TB"


def progress_bar(percent, length=12):
    filled = int(length * percent / 100)
    return "█" * filled + "░" * (length - filled)


async def send_log(text: str):
    if not LOG_CHANNEL:
        return
    try:
        await app.send_message(LOG_CHANNEL, text)
    except Exception as e:
        logger.error(f"Log error: {e}")


# ══════════════════════════════════════
#           BASIC COMMANDS
# ══════════════════════════════════════

@app.on_message(filters.command("start"))
async def start_cmd(client, message: Message):
    await message.reply_text(
        "**🎬 YouTube Auto Uploader Bot**\n\n"
        "Video bhejo → YouTube pe upload → Link milega!\n\n"
        "**Formats:** MP4, MKV, WebM, AVI, MOV\n"
        "**Mode:** Unlisted 🔒\n"
        "**Multi-Account:** Auto-rotate ✅\n\n"
        "Caption = YouTube title 📌\n\n"
        "/accounts — Connected accounts dekho\n"
        "/links — Recent uploads\n"
        "/stats — Upload statistics"
    )


@app.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    await message.reply_text(
        "**📖 Commands**\n\n"
        "**👤 Account Management (Owner)**\n"
        "/addaccount NAME — Naya YT account add karo\n"
        "/code NAME CODE — Auth code submit karo\n"
        "/removeaccount NAME — Account remove karo\n"
        "/accounts — Sare accounts dekho\n\n"
        "**📊 Data Commands**\n"
        "/links — Last 10 uploads\n"
        "/search TITLE — Video dhundho\n"
        "/stats — Total stats\n\n"
        "**📹 Upload**\n"
        "Bas video bhejo — auto upload hoga!"
    )


# ══════════════════════════════════════
#        ACCOUNT MANAGEMENT
# ══════════════════════════════════════

@app.on_message(filters.command("addaccount") & filters.user(OWNER_ID))
async def add_account_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text(
            "**Usage:** `/addaccount ACCOUNT_NAME`\n\n"
            "**Example:**\n"
            "`/addaccount ACC1`\n"
            "`/addaccount ACC2`\n"
            "`/addaccount MyChannel`"
        )
        return

    acc_name = parts[1].strip().replace(" ", "_")

    if acc_name in youtube.accounts:
        await message.reply_text(f"⚠️ Account `{acc_name}` already exist karta hai!")
        return

    auth_url = youtube.start_auth(acc_name)
    if auth_url:
        pending_add[OWNER_ID] = acc_name
        await message.reply_text(
            f"**🔐 Account `{acc_name}` Add karo**\n\n"
            f"**Step 1:** Iss link pe jao:\n`{auth_url}`\n\n"
            f"**Step 2:** Google account se login karo\n"
            f"**Step 3:** Allow karo → Code copy karo\n"
            f"**Step 4:** Bot ko bhejo:\n"
            f"`/code {acc_name} YAHAN_CODE_DAALO`\n\n"
            f"⏰ Code 2 minute me expire hota hai — jaldi karo!"
        )
    else:
        await message.reply_text("❌ Auth URL generate nahi hua. Dobara try karo.")


@app.on_message(filters.command("code") & filters.user(OWNER_ID))
async def code_cmd(client, message: Message):
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        await message.reply_text(
            "**Usage:** `/code ACCOUNT_NAME AUTH_CODE`\n\n"
            "**Example:** `/code ACC1 4/0AX9abc...`"
        )
        return

    acc_name = parts[1].strip()
    code = parts[2].strip()

    await message.reply_text(f"⏳ Account `{acc_name}` authorize ho raha hai...")

    success = youtube.finish_auth(acc_name, code)
    if success:
        await message.reply_text(
            f"✅ **Account `{acc_name}` Successfully Added!**\n\n"
            f"Total accounts: `{youtube.get_account_count()}`\n\n"
            f"{youtube.get_accounts_status()}"
        )
    else:
        await message.reply_text(
            f"❌ **Authorization Failed!**\n\n"
            f"Code galat ya expire ho gaya.\n"
            f"Dobara `/addaccount {acc_name}` karo aur jaldi code bhejo."
        )


@app.on_message(filters.command("removeaccount") & filters.user(OWNER_ID))
async def remove_account_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text(
            "**Usage:** `/removeaccount ACCOUNT_NAME`\n\n"
            f"**Current accounts:**\n{youtube.get_accounts_status()}"
        )
        return

    acc_name = parts[1].strip()
    success = youtube.remove_account(acc_name)
    if success:
        await message.reply_text(
            f"✅ Account `{acc_name}` remove ho gaya!\n\n"
            f"Remaining accounts: `{youtube.get_account_count()}`\n"
            f"{youtube.get_accounts_status()}"
        )
    else:
        await message.reply_text(f"❌ Account `{acc_name}` nahi mila.")


@app.on_message(filters.command("accounts"))
async def accounts_cmd(client, message: Message):
    count = youtube.get_account_count()
    status = youtube.get_accounts_status()
    daily_limit = count * 6
    await message.reply_text(
        f"**📋 YouTube Accounts**\n\n"
        f"{status}\n\n"
        f"**Total:** `{count}` accounts\n"
        f"**Daily Limit:** ~`{daily_limit}` videos/day"
    )


# ══════════════════════════════════════
#           DATABASE COMMANDS
# ══════════════════════════════════════

@app.on_message(filters.command("links"))
async def links_cmd(client, message: Message):
    videos = await db.get_recent_videos(10)
    if not videos:
        await message.reply_text("❌ Abhi tak koi video upload nahi hua.")
        return
    text = "**📋 Last 10 Uploaded Videos:**\n\n"
    for i, v in enumerate(videos, 1):
        text += f"{i}. **{v['title']}**\n🔗 {v['yt_link']}\n📦 {v['size_mb']:.1f} MB\n\n"
    await message.reply_text(text)


@app.on_message(filters.command("search"))
async def search_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text("Usage: `/search VIDEO TITLE`")
        return
    query = parts[1].strip()
    videos = await db.search_videos(query)
    if not videos:
        await message.reply_text(f"❌ `{query}` se koi video nahi mila.")
        return
    text = f"**🔍 Search: `{query}`**\n\n"
    for i, v in enumerate(videos[:10], 1):
        text += f"{i}. **{v['title']}**\n🔗 {v['yt_link']}\n📦 {v['size_mb']:.1f} MB\n\n"
    await message.reply_text(text)


@app.on_message(filters.command("stats"))
async def stats_cmd(client, message: Message):
    total = await db.get_total_count()
    total_size = await db.get_total_size()
    acc_count = youtube.get_account_count()
    await message.reply_text(
        f"**📊 Bot Stats**\n\n"
        f"🎬 Total Videos: `{total}`\n"
        f"💾 Total Size: `{total_size:.1f} MB`\n"
        f"👤 Active Accounts: `{acc_count}`\n"
        f"📈 Daily Capacity: `~{acc_count * 6}` videos"
    )


# ══════════════════════════════════════
#           VIDEO HANDLER
# ══════════════════════════════════════

@app.on_message(filters.video | filters.document)
async def handle_video(client, message: Message):
    file = message.video or message.document
    if not file:
        return

    if message.document:
        mime = getattr(file, 'mime_type', '') or ''
        fname = getattr(file, 'file_name', '') or ''
        ext = os.path.splitext(fname)[1].lower()
        if mime not in SUPPORTED_MIME and ext not in SUPPORTED_EXT:
            await message.reply_text("❌ Sirf video files bhejo (MP4, MKV, etc.)")
            return

    if youtube.get_account_count() == 0:
        await message.reply_text(
            "❌ Koi YouTube account connected nahi hai!\n\n"
            "Owner ko `/addaccount ACC1` use karna hoga."
        )
        return

    caption = (
        message.caption
        or (message.document.file_name if message.document else None)
        or f"Video_{message.id}"
    )
    title = caption.strip()
    for ext in SUPPORTED_EXT:
        if title.lower().endswith(ext):
            title = title[:-len(ext)]
            break

    file_size = file.file_size or 0
    size_mb = file_size / (1024 * 1024)
    acc_count = youtube.get_account_count()

    status_msg = await message.reply_text(
        f"**📥 Download shuru...**\n\n"
        f"📌 `{title}`\n"
        f"📦 `{size_mb:.1f} MB`\n"
        f"👤 Accounts: `{acc_count}` (auto-rotate ON)"
    )

    tmp_dir = tempfile.mkdtemp()
    video_path = None
    dl_start = time.time()
    last_edit = [0.0]

    async def download_progress(current, total):
        now = time.time()
        if now - last_edit[0] < 3:
            return
        last_edit[0] = now
        elapsed = now - dl_start
        speed = current / elapsed if elapsed > 0 else 0
        percent = (current / total * 100) if total else 0
        eta = int((total - current) / speed) if speed > 0 else 0
        bar = progress_bar(percent)
        try:
            await status_msg.edit_text(
                f"**📥 Downloading...**\n\n"
                f"📌 `{title}`\n"
                f"`[{bar}]` `{percent:.1f}%`\n\n"
                f"📤 `{human_size(current)}` / `{human_size(total)}`\n"
                f"⚡ Speed: `{human_size(speed)}/s`\n"
                f"⏳ ETA: `{eta}s`"
            )
        except Exception:
            pass

    try:
        video_path = await client.download_media(
            message,
            file_name=os.path.join(tmp_dir, f"video_{message.id}.tmp"),
            progress=download_progress
        )

        dl_time = time.time() - dl_start
        dl_speed = file_size / dl_time if dl_time > 0 else 0

        await status_msg.edit_text(
            f"**✅ Download Done!**\n\n"
            f"📌 `{title}`\n"
            f"📦 `{size_mb:.1f} MB` in `{dl_time:.1f}s`\n"
            f"⚡ Avg: `{human_size(dl_speed)}/s`\n\n"
            f"**⬆️ YouTube upload shuru... (Auto-rotate ON)**"
        )

        ul_start = time.time()
        progress_queue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        upload_future = loop.run_in_executor(
            None,
            lambda: youtube.upload_video(
                file_path=video_path,
                title=title,
                description=f"Uploaded via Telegram Bot\nCaption: {caption}",
                privacy="unlisted",
                progress_queue=progress_queue,
                loop=loop
            )
        )

        last_ul_edit = [0.0]
        while not upload_future.done():
            try:
                item = await asyncio.wait_for(progress_queue.get(), timeout=3)
                if item is None:
                    break
                percent, uploaded, total_bytes = item
                now = time.time()
                if now - last_ul_edit[0] < 3:
                    continue
                last_ul_edit[0] = now
                elapsed = now - ul_start
                speed = uploaded / elapsed if elapsed > 0 else 0
                eta = int((total_bytes - uploaded) / speed) if speed > 0 else 0
                bar = progress_bar(percent)
                try:
                    await status_msg.edit_text(
                        f"**⬆️ Uploading to YouTube...**\n\n"
                        f"📌 `{title}`\n"
                        f"`[{bar}]` `{percent:.1f}%`\n\n"
                        f"📤 `{human_size(uploaded)}` / `{human_size(total_bytes)}`\n"
                        f"⚡ Speed: `{human_size(speed)}/s`\n"
                        f"⏳ ETA: `{eta}s`"
                    )
                except Exception:
                    pass
            except asyncio.TimeoutError:
                continue

        yt_link, yt_id = await upload_future
        ul_time = time.time() - ul_start
        ul_speed = file_size / ul_time if ul_time > 0 else 0

        if yt_link:
            await db.save_video(
                title=title,
                caption=caption,
                yt_link=yt_link,
                yt_id=yt_id,
                size_mb=size_mb,
                user_id=message.from_user.id if message.from_user else 0,
                username=message.from_user.username if message.from_user else "unknown"
            )
            await status_msg.edit_text(
                f"**✅ Upload Successful!** 🎉\n\n"
                f"📌 Title: `{title}`\n"
                f"📦 Size: `{size_mb:.1f} MB`\n\n"
                f"📥 Download: `{dl_time:.1f}s` @ `{human_size(dl_speed)}/s`\n"
                f"📤 Upload: `{ul_time:.1f}s` @ `{human_size(ul_speed)}/s`\n\n"
                f"🔗 **Link:**\n{yt_link}\n\n"
                f"🔒 Unlisted — Sirf link se open hoga\n"
                f"💾 MongoDB me save ✅"
            )
        else:
            await status_msg.edit_text(
                "❌ **Sare accounts ka limit exceed ho gaya!**\n\n"
                f"Total accounts: `{youtube.get_account_count()}`\n"
                "Kal dobara try karo ya naya account add karo:\n"
                "`/addaccount ACC_NEW`"
            )

    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        try:
            await status_msg.edit_text(f"❌ Error: `{str(e)[:300]}`")
        except Exception:
            pass
    finally:
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except Exception:
                pass
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ══════════════════════════════════════
#           BOT STARTUP
# ══════════════════════════════════════

async def main():
    await app.start()

    await app.set_bot_commands([
        BotCommand("start", "Bot shuru karo"),
        BotCommand("help", "Sare commands dekho"),
        BotCommand("accounts", "Connected YT accounts dekho"),
        BotCommand("addaccount", "Naya YouTube account add karo"),
        BotCommand("removeaccount", "YouTube account remove karo"),
        BotCommand("code", "Auth code submit karo"),
        BotCommand("links", "Last 10 uploaded videos"),
        BotCommand("search", "Video title se search karo"),
        BotCommand("stats", "Bot ki total statistics"),
    ])

    logger.info("✅ Bot ready! Messages ka intezaar hai...")

    # Bot ko hamesha jagraat rakhta hai — jab tak manually band na karo
    await asyncio.Event().wait()

    await app.stop()


if __name__ == "__main__":
    Thread(target=start_health_server, daemon=True).start()
    logger.info("Bot starting...")
    asyncio.run(main())
