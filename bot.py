import os
import asyncio
import logging
import time
import tempfile
import shutil
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

from youtube_uploader import YouTubeUploader
from database import Database

# ============ CONFIG (Environment Variables) ============
BOT_TOKEN  = os.environ["BOT_TOKEN"]
API_ID     = int(os.environ["API_ID"])
API_HASH   = os.environ["API_HASH"]
MONGO_URI  = os.environ["MONGO_URI"]
OWNER_ID   = int(os.environ["OWNER_ID"])
PORT       = int(os.environ.get("PORT", 8080))  # Render PORT
LOG_CHANNEL = os.environ.get("LOG_CHANNEL")     # Optional
# =======================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Render ke liye Health Check HTTP Server ───────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *args):
        pass  # HTTP logs band karo

def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    logger.info(f"Health server port {PORT} pe chal raha hai")
    server.serve_forever()
# ──────────────────────────────────────────────────────────────────────────────

app = Client(
    "yt_uploader_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=8,
    max_concurrent_transmissions=8,
)

db = Database(MONGO_URI)
youtube = YouTubeUploader(mongo_uri=MONGO_URI)

SUPPORTED_MIME = [
    "video/mp4", "video/x-matroska", "video/webm",
    "video/avi", "video/quicktime", "video/x-msvideo",
    "video/mpeg", "video/3gpp"
]
SUPPORTED_EXT = [".mp4", ".mkv", ".webm", ".avi", ".mov", ".mpeg", ".3gp"]


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
        await app.send_message(int(LOG_CHANNEL), text)
    except Exception as e:
        logger.error(f"Log error: {e}")


# ─── Commands ──────────────────────────────────────────────────────────────────

@app.on_message(filters.command("start"))
async def start_cmd(client, message: Message):
    auth_status = "✅ Authorized" if youtube.is_authorized() else "❌ Not Authorized"
    await message.reply_text(
        f"**🎬 YouTube Auto Uploader Bot**\n\n"
        f"Video bhejo → YouTube pe upload → Link milega!\n\n"
        f"**Formats:** MP4, MKV, WebM, AVI, MOV\n"
        f"**Mode:** Unlisted 🔒\n"
        f"**YouTube:** {auth_status}\n\n"
        f"Caption = YouTube title 📌"
    )


@app.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    await message.reply_text(
        "**📖 Help**\n\n"
        "1️⃣ Video send/forward karo\n"
        "2️⃣ Live speed + progress dikhega\n"
        "3️⃣ YouTube unlisted link milega\n\n"
        "/auth — YouTube authorize (owner only)\n"
        "/code CODE — Auth code dalo (owner only)\n"
        "/status — Bot status dekho"
    )


@app.on_message(filters.command("status") & filters.user(OWNER_ID))
async def status_cmd(client, message: Message):
    total = await db.get_total_count()
    size = await db.get_total_size()
    auth = "✅ Authorized" if youtube.is_authorized() else "❌ Not Authorized"
    await message.reply_text(
        f"**📊 Bot Status**\n\n"
        f"🎬 Total Videos: `{total}`\n"
        f"📦 Total Size: `{size:.1f} MB`\n"
        f"🔑 YouTube: {auth}"
    )


@app.on_message(filters.command("auth") & filters.user(OWNER_ID))
async def auth_cmd(client, message: Message):
    if youtube.is_authorized():
        await message.reply_text(
            "✅ **YouTube pehle se authorized hai!**\n\n"
            "Dobara auth karna ho to `/reauth` karo."
        )
        return
    auth_url = youtube.get_auth_url()
    if auth_url:
        await message.reply_text(
            f"**🔑 YouTube Auth Required!**\n\n"
            f"Neeche link pe jao, allow karo, code copy karo:\n\n"
            f"`{auth_url}`\n\n"
            f"Phir bhejo: `/code YAHAN_CODE_LIKHO`"
        )
    else:
        await message.reply_text("❌ Auth URL generate nahi ho saka.")


@app.on_message(filters.command("reauth") & filters.user(OWNER_ID))
async def reauth_cmd(client, message: Message):
    await db.delete_yt_token()
    youtube.service = None
    auth_url = youtube.get_auth_url()
    if auth_url:
        await message.reply_text(
            f"**🔄 Re-Auth shuru...**\n\n"
            f"`{auth_url}`\n\n"
            f"Code milne par: `/code YAHAN_CODE_LIKHO`"
        )


@app.on_message(filters.command("code") & filters.user(OWNER_ID))
async def code_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text("Usage: `/code YOUR_AUTH_CODE`")
        return
    msg = await message.reply_text("⏳ Verify ho raha hai...")
    success = youtube.authenticate_with_code(parts[1].strip())
    if success:
        await msg.edit_text(
            "✅ **YouTube authorized ho gaya!**\n\n"
            "Token MongoDB mein permanently save hai.\n"
            "Ab Render restart pe bhi dobara auth nahi karna padega! 🎉"
        )
    else:
        await msg.edit_text("❌ Failed. Code galat hai ya expire ho gaya. `/auth` se dobara try karo.")


# ─── Video Handler ─────────────────────────────────────────────────────────────

@app.on_message(filters.video | filters.document)
async def handle_video(client, message: Message):
    # YouTube authorized check
    if not youtube.is_authorized():
        await message.reply_text(
            "❌ **YouTube authorized nahi hai!**\n\n"
            "Owner ko `/auth` karna hoga pehle."
        )
        return

    file = message.video or message.document
    if not file:
        return

    # Document type check
    if message.document:
        mime = getattr(file, 'mime_type', '') or ''
        fname = getattr(file, 'file_name', '') or ''
        ext = os.path.splitext(fname)[1].lower()
        if mime not in SUPPORTED_MIME and ext not in SUPPORTED_EXT:
            await message.reply_text("❌ Sirf video files bhejo (MP4, MKV, etc.)")
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

    # ─── Duplicate Check ──────────────────────────────────────────────────────
    existing = await db.is_duplicate(caption)
    if existing:
        await message.reply_text(
            f"⚠️ **Yeh video pehle upload ho chuki hai!**\n\n"
            f"📌 Title: `{existing['title']}`\n"
            f"🔗 **Link:** {existing['yt_link']}\n\n"
            f"_Same caption wali video dobara upload nahi hogi._"
        )
        return
    # ─────────────────────────────────────────────────────────────────────────

    file_size = file.file_size or 0
    size_mb = file_size / (1024 * 1024)

    status_msg = await message.reply_text(
        f"**📥 Download shuru...**\n\n"
        f"📌 `{title}`\n📦 `{size_mb:.1f} MB`"
    )

    await send_log(
        f"**📥 New Request**\n"
        f"👤 {message.from_user.mention if message.from_user else 'N/A'}\n"
        f"📌 `{title}` | `{size_mb:.1f} MB`"
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

    yt_link = None
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
            f"**⬆️ YouTube upload shuru...**"
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
            # ─── Database mein save karo ──────────────────────────────────────
            try:
                await db.save_video(
                    title=title,
                    caption=caption,
                    yt_link=yt_link,
                    yt_id=yt_id,
                    size_mb=size_mb,
                    user_id=message.from_user.id if message.from_user else 0,
                    username=message.from_user.username if message.from_user else "unknown"
                )
            except Exception as db_err:
                logger.error(f"DB save error: {db_err}")
            # ─────────────────────────────────────────────────────────────────

            await status_msg.edit_text(
                f"**✅ Upload Successful!** 🎉\n\n"
                f"📌 Title: `{title}`\n"
                f"📦 Size: `{size_mb:.1f} MB`\n\n"
                f"📥 Download: `{dl_time:.1f}s` @ `{human_size(dl_speed)}/s`\n"
                f"📤 Upload: `{ul_time:.1f}s` @ `{human_size(ul_speed)}/s`\n\n"
                f"🔗 **Link:**\n{yt_link}\n\n"
                f"🔒 Unlisted — Sirf link wale dekh sakte hain"
            )

            await send_log(
                f"**✅ Done**\n"
                f"👤 {message.from_user.mention if message.from_user else 'N/A'}\n"
                f"📌 `{title}`\n🔗 {yt_link}"
            )
        else:
            await status_msg.edit_text("❌ Upload failed! `/auth` se re-authorize karo.")

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


if __name__ == "__main__":
    # Render health server background thread mein start karo
    Thread(target=start_health_server, daemon=True).start()
    logger.info("Bot starting...")
    app.run()