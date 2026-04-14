import os
import asyncio
import logging
import time
import tempfile
import shutil
import json
import urllib.request
from datetime import datetime, timedelta, timezone
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait

from youtube_uploader import YouTubeUploader
from database import Database

# ============ CONFIG ============
BOT_TOKEN   = os.environ["BOT_TOKEN"]
API_ID      = int(os.environ["API_ID"])
API_HASH    = os.environ["API_HASH"]
MONGO_URI   = os.environ["MONGO_URI"]
OWNER_ID    = int(os.environ["OWNER_ID"])
PORT        = int(os.environ.get("PORT", 8080))
LOG_CHANNEL = os.environ.get("LOG_CHANNEL")
ADMIN_LINK  = os.environ.get("ADMIN_LINK", "")

IST = timezone(timedelta(hours=5, minutes=30))   # Indian Standard Time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============ HEALTH SERVER ============
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *args): pass

def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    logger.info(f"Health server port {PORT} pe chal raha hai")
    server.serve_forever()

# ============ CLIENT ============
app = Client(
    "railway_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=8,
    max_concurrent_transmissions=8,
)
youtube = YouTubeUploader()
db      = Database(MONGO_URI)

# ============ SUPPORTED FORMATS ============
SUPPORTED_MIME = [
    "video/mp4","video/x-matroska","video/webm",
    "video/avi","video/quicktime","video/x-msvideo",
    "video/mpeg","video/3gpp",
]
SUPPORTED_EXT = [".mp4",".mkv",".webm",".avi",".mov",".mpeg",".3gp"]

# ============ GLOBAL STATE ============
upload_queue   = asyncio.Queue()   # unlimited — 100-200 videos support
active_uploads = {}                 # file_unique_id -> True
UPLOAD_WORKERS = 2                  # ek saath 2 workers

stop_flag      = False              # admin /stop se set hota hai
quota_exceeded = False              # YouTube quota khatam hone pe set

# Per-chat progress: chat_id -> dict
chat_progress  = {}

# Check mode: user_id -> {"results": [(ok, caption, yt_link), ...]}
check_mode     = {}

# Contact flow
contact_state     = {}   # user_id -> "premium" | "message"
contact_reply_map = {}   # admin_msg_id -> user_id

# /addaccount flow
pending_add = {}

# ============ HELPERS ============
def human_size(num):
    for unit in ["B", "KB", "MB", "GB"]:
        if num < 1024:
            return f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TB"

def progress_bar(pct, length=12):
    f = int(length * pct / 100)
    return "█" * f + "░" * (length - f)

async def send_log(text: str):
    if not LOG_CHANNEL:
        return
    try:
        await app.send_message(LOG_CHANNEL, text)
    except Exception as e:
        logger.error(f"Log error: {e}")

async def register_user(message: Message):
    if not message.from_user:
        return
    u = message.from_user
    await db.save_user(u.id, u.username or "", u.first_name or "")

def contact_keyboard():
    btns = [
        [InlineKeyboardButton("⭐ Premium Lena Hai (Free)", callback_data="contact_premium")],
        [InlineKeyboardButton("💬 Kuch Aur Poochna Hai",   callback_data="contact_message")],
    ]
    if ADMIN_LINK:
        btns.append([InlineKeyboardButton("📲 Admin se Seedha Baat Karo", url=ADMIN_LINK)])
    return InlineKeyboardMarkup(btns)

def set_bot_commands_via_api():
    cmds = [
        {"command": "start",         "description": "Bot shuru karo"},
        {"command": "help",          "description": "Sare commands dekho"},
        {"command": "check",         "description": "Videos upload hain ya nahi check karo"},
        {"command": "checkdone",     "description": "Check mode band karo, result dekho"},
        {"command": "accounts",      "description": "Connected YT accounts dekho"},
        {"command": "addaccount",    "description": "Apna YouTube account add karo (Premium)"},
        {"command": "code",          "description": "Auth code submit karo"},
        {"command": "links",         "description": "Last 10 uploaded videos"},
        {"command": "search",        "description": "Video title se search karo"},
        {"command": "stats",         "description": "Bot ki total statistics"},
        {"command": "botstats",      "description": "Daily/Weekly/Monthly user stats"},
        {"command": "mypremium",     "description": "Apna premium status dekho"},
        {"command": "contact",       "description": "Premium lo ya admin se baat karo"},
        {"command": "retry",         "description": "Pending uploads manually retry karo"},
        {"command": "pending",       "description": "Pending uploads dekho (Admin)"},
        {"command": "addpremium",    "description": "User ko premium do (Admin)"},
        {"command": "removepremium", "description": "User ka premium hato (Admin)"},
        {"command": "premiumlist",   "description": "Sare premium users (Admin)"},
        {"command": "broadcast",     "description": "Sab users ko message bhejo (Admin)"},
        {"command": "reply",         "description": "User ko jawab bhejo (Admin)"},
        {"command": "stop",          "description": "Saare uploads band karo (Admin)"},
        {"command": "resume",        "description": "Uploads dobara shuru karo (Admin)"},
        {"command": "clearquota",    "description": "YouTube quota flag reset karo (Admin)"},
    ]
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands"
    data = json.dumps({"commands": cmds}).encode()
    req  = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        resp   = urllib.request.urlopen(req)
        result = json.loads(resp.read())
        if result.get("ok"):
            logger.info("✅ Commands menu set ho gaya!")
    except Exception as e:
        logger.error(f"Commands API error: {e}")

# ══════════════════════════════════════════════════════════
# PINNED PROGRESS MESSAGE (bulk uploads ke liye)
# ══════════════════════════════════════════════════════════

async def _progress_text(chat_id: int) -> str:
    info      = chat_progress.get(chat_id, {})
    total     = info.get("total", 0)
    done      = info.get("done",  0)
    failed    = info.get("failed", 0)
    current   = info.get("current", "Starting...")
    remaining = max(0, total - done - failed)

    bar_f = int(12 * done / total) if total > 0 else 0
    bar   = "█" * bar_f + "░" * (12 - bar_f)
    pct   = int(100 * done / total) if total > 0 else 0

    return (
        f"📊 **Queue Progress**\n"
        f"`[{bar}]` `{pct}%`\n\n"
        f"✅ Done      : `{done}`\n"
        f"❌ Failed    : `{failed}`\n"
        f"⏳ Remaining : `{remaining}`\n"
        f"📋 Total     : `{total}`\n\n"
        f"🎬 **Ab ho raha hai:**\n`{current}`"
    )

async def update_progress_pin(chat_id: int, current_title: str = ""):
    if chat_id not in chat_progress:
        return
    info = chat_progress[chat_id]
    if current_title:
        info["current"] = current_title

    # Single video ke liye pin nahi (sirf bulk pe)
    if info["total"] <= 1:
        return

    text = await _progress_text(chat_id)
    try:
        if info.get("pinned_msg_id"):
            await app.edit_message_text(chat_id, info["pinned_msg_id"], text)
        else:
            msg = await app.send_message(chat_id, text)
            info["pinned_msg_id"] = msg.id
            try:
                await app.pin_chat_message(chat_id, msg.id, disable_notification=True)
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"Progress pin update error: {e}")

async def finish_progress_pin(chat_id: int):
    info = chat_progress.get(chat_id)
    if not info:
        return

    total  = info.get("total",  0)
    done   = info.get("done",   0)
    failed = info.get("failed", 0)

    if total > 1 and info.get("pinned_msg_id"):
        text = (
            f"✅ **Sab Upload Complete!**\n\n"
            f"📋 Total   : `{total}`\n"
            f"✅ Uploaded: `{done}`\n"
            f"❌ Failed  : `{failed}`\n\n"
            f"_(Yeh message 1 minute mein unpin ho jaayega)_"
        )
        try:
            await app.edit_message_text(chat_id, info["pinned_msg_id"], text)
            await asyncio.sleep(60)
            await app.unpin_chat_message(chat_id, info["pinned_msg_id"])
        except Exception:
            pass

    chat_progress.pop(chat_id, None)

# ══════════════════════════════════════════════════════════
# QUOTA EXCEEDED — sab kuch rokna
# ══════════════════════════════════════════════════════════

async def handle_quota_exceeded():
    """
    Jaise hi quota khatam ho:
    1. quota_exceeded flag set karo (DB + memory)
    2. Queue mein jo bhi hai unhe pending mein save karo
    3. Owner ko notify karo
    4. Batao ki 2 PM IST pe auto retry hoga
    """
    global quota_exceeded
    quota_exceeded = True
    await db.set_setting("quota_exceeded", True)

    # Queue drain kar ke pending mein save karo
    saved = 0
    while not upload_queue.empty():
        try:
            task = upload_queue.get_nowait()
            _, message, _, file_unique_id, title, caption, file_size, size_mb = task
            user_id  = message.from_user.id if message.from_user else 0
            username = (message.from_user.username or "") if message.from_user else ""
            await db.save_pending_upload(
                chat_id=message.chat.id,
                message_id=message.id,
                title=title,
                caption=caption,
                file_size=file_size,
                size_mb=size_mb,
                user_id=user_id,
                username=username,
                file_unique_id=file_unique_id,
            )
            saved += 1
            upload_queue.task_done()
        except Exception:
            break

    now_ist   = datetime.now(IST)
    retry_ist = now_ist.replace(hour=14, minute=0, second=0, microsecond=0)
    if now_ist >= retry_ist:
        retry_ist += timedelta(days=1)
    hours_left = (retry_ist - now_ist).total_seconds() / 3600

    msg = (
        f"⚠️ **YouTube Quota Khatam Ho Gaya!**\n\n"
        f"📦 Queue se `{saved}` videos pending mein save kar liye gaye.\n\n"
        f"🕑 **Auto retry:** Kal **2:00 PM IST** ({hours_left:.1f} ghante baad)\n"
        f"▶️ **Manual retry:** `/retry` command use karo\n\n"
        f"_Dobara video bhejne ki zaroorat nahi — sab save hai!_"
    )
    try:
        await app.send_message(OWNER_ID, msg)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════
# RETRY PENDING UPLOADS
# ══════════════════════════════════════════════════════════

async def retry_pending_uploads(triggered_by: int = None):
    """Pending uploads ko queue mein daalo."""
    global quota_exceeded, stop_flag

    pending = await db.get_pending_uploads()
    if not pending:
        if triggered_by:
            try:
                await app.send_message(triggered_by, "✅ Koi pending upload nahi hai.")
            except Exception:
                pass
        return

    quota_exceeded = False
    stop_flag      = False
    await db.set_setting("quota_exceeded", False)

    notify_msg = (
        f"🔄 **Pending Retry Shuru**\n\n"
        f"📋 Total pending: `{len(pending)}`\n"
        f"⏳ Sab queue mein daal diye gaye..."
    )
    if triggered_by:
        try:
            await app.send_message(triggered_by, notify_msg)
        except Exception:
            pass

    for doc in pending:
        chat_id    = doc["chat_id"]
        message_id = doc["message_id"]
        title      = doc["title"]
        caption    = doc["caption"]
        file_size  = doc["file_size"]
        size_mb    = doc["size_mb"]
        user_id    = doc["user_id"]
        username   = doc["username"]
        file_uid   = doc.get("file_unique_id", "")
        doc_id     = doc["_id"]

        # Already active mein hai?
        if file_uid and file_uid in active_uploads:
            continue

        try:
            # Original message se video phir se bhejne ki koshish
            msg = await app.get_messages(chat_id, message_id)
            if not msg or (not msg.video and not msg.document):
                await db.delete_pending_upload(doc_id)
                continue
        except Exception:
            await db.delete_pending_upload(doc_id)
            continue

        # Progress tracking
        if chat_id not in chat_progress:
            chat_progress[chat_id] = {
                "total": 0, "done": 0, "failed": 0,
                "current": "", "pinned_msg_id": None,
            }
        chat_progress[chat_id]["total"] += 1

        status_msg = await app.send_message(
            chat_id,
            f"🔄 **Retry Queue mein daal diya**\n📌 `{title}`"
        )

        active_uploads[file_uid or str(message_id)] = True
        await upload_queue.put((
            app, msg, status_msg,
            file_uid or str(message_id),
            title, caption, file_size, size_mb,
        ))
        await db.delete_pending_upload(doc_id)
        await asyncio.sleep(0.1)

# ══════════════════════════════════════════════════════════
# AUTO RETRY SCHEDULER (2 PM IST daily)
# ══════════════════════════════════════════════════════════

async def auto_retry_scheduler():
    logger.info("Auto-retry scheduler start ho gaya (target: 2 PM IST daily)")
    while True:
        now_ist   = datetime.now(IST)
        target    = now_ist.replace(hour=14, minute=0, second=0, microsecond=0)
        if now_ist >= target:
            target += timedelta(days=1)

        wait_sec = (target - now_ist).total_seconds()
        logger.info(f"Next auto-retry: {target.strftime('%Y-%m-%d %H:%M IST')} ({wait_sec/3600:.1f}h baad)")
        await asyncio.sleep(wait_sec)

        pending_count = await db.get_pending_count()
        if pending_count > 0:
            logger.info(f"Auto-retry: {pending_count} pending uploads retry ho rahe hain")
            await send_log(f"🕑 **Auto Retry** — 2 PM IST\n📋 {pending_count} pending videos retry ho rahe hain...")
            await retry_pending_uploads(triggered_by=OWNER_ID)
        else:
            logger.info("Auto-retry: Koi pending nahi, skip.")

# ══════════════════════════════════════════════════════════
# UPLOAD CORE
# ══════════════════════════════════════════════════════════

async def process_upload(client, message: Message, status_msg,
                         file_unique_id, title, caption, file_size, size_mb):
    global stop_flag, quota_exceeded
    chat_id = message.chat.id

    # Stop / quota check
    if stop_flag or quota_exceeded:
        reason = "Admin ne uploads band kar diye" if stop_flag else "YouTube quota khatam"
        active_uploads.pop(file_unique_id, None)
        # Save to pending
        await db.save_pending_upload(
            chat_id=chat_id, message_id=message.id, title=title, caption=caption,
            file_size=file_size, size_mb=size_mb,
            user_id=message.from_user.id if message.from_user else 0,
            username=(message.from_user.username or "") if message.from_user else "",
            file_unique_id=file_unique_id,
        )
        if chat_id in chat_progress:
            chat_progress[chat_id]["failed"] += 1
        try:
            await status_msg.edit_text(
                f"🚫 **Upload Roka Gaya**\n\n"
                f"📌 `{title}`\n"
                f"📝 Reason: {reason}\n"
                f"💾 Pending mein save kar liya — `/retry` pe upload hoga."
            )
        except Exception:
            pass
        await update_progress_pin(chat_id)
        return

    tmp_dir    = tempfile.mkdtemp()
    video_path = None
    dl_start   = time.time()
    last_edit  = [0.0]

    # Update progress pin with current video
    await update_progress_pin(chat_id, title)

    async def download_progress(current, total):
        now = time.time()
        if now - last_edit[0] < 3:
            return
        last_edit[0] = now
        elapsed = now - dl_start
        speed   = current / elapsed if elapsed > 0 else 0
        pct     = (current / total * 100) if total else 0
        eta     = int((total - current) / speed) if speed > 0 else 0
        bar     = progress_bar(pct)
        try:
            await status_msg.edit_text(
                f"**📥 Downloading...**\n\n"
                f"📌 `{title}`\n"
                f"`[{bar}]` `{pct:.1f}%`\n\n"
                f"📤 `{human_size(current)}` / `{human_size(total)}`\n"
                f"⚡ Speed: `{human_size(speed)}/s`\n"
                f"⏳ ETA: `{eta}s`"
            )
        except Exception:
            pass

    try:
        await status_msg.edit_text(
            f"**📥 Download shuru...**\n\n"
            f"📌 `{title}`\n"
            f"📦 `{size_mb:.1f} MB`\n"
            f"👤 Accounts: `{youtube.get_account_count()}` (auto-rotate ON)"
        )

        video_path = await client.download_media(
            message,
            file_name=os.path.join(tmp_dir, f"video_{message.id}.tmp"),
            progress=download_progress,
        )

        dl_time  = time.time() - dl_start
        dl_speed = file_size / dl_time if dl_time > 0 else 0

        await status_msg.edit_text(
            f"**✅ Download Done!**\n\n"
            f"📌 `{title}`\n"
            f"📦 `{size_mb:.1f} MB` in `{dl_time:.1f}s`\n"
            f"⚡ Avg: `{human_size(dl_speed)}/s`\n\n"
            f"**⬆️ YouTube upload shuru... (Auto-rotate ON)**"
        )

        ul_start       = time.time()
        progress_queue = asyncio.Queue()
        loop           = asyncio.get_event_loop()

        upload_future = loop.run_in_executor(
            None,
            lambda: youtube.upload_video(
                file_path=video_path,
                title=title,
                description=f"Uploaded via Telegram Bot\nCaption: {caption}",
                privacy="unlisted",
                progress_queue=progress_queue,
                loop=loop,
            ),
        )

        last_ul_edit = [0.0]
        while not upload_future.done():
            try:
                item = await asyncio.wait_for(progress_queue.get(), timeout=3)
                if item is None:
                    break
                pct, uploaded_bytes, total_bytes = item
                now = time.time()
                if now - last_ul_edit[0] < 3:
                    continue
                last_ul_edit[0] = now
                elapsed = now - ul_start
                speed   = uploaded_bytes / elapsed if elapsed > 0 else 0
                eta     = int((total_bytes - uploaded_bytes) / speed) if speed > 0 else 0
                bar     = progress_bar(pct)
                try:
                    await status_msg.edit_text(
                        f"**⬆️ Uploading to YouTube...**\n\n"
                        f"📌 `{title}`\n"
                        f"`[{bar}]` `{pct:.1f}%`\n\n"
                        f"📤 `{human_size(uploaded_bytes)}` / `{human_size(total_bytes)}`\n"
                        f"⚡ Speed: `{human_size(speed)}/s`\n"
                        f"⏳ ETA: `{eta}s`"
                    )
                except Exception:
                    pass
            except asyncio.TimeoutError:
                continue

        yt_link, yt_id, status = await upload_future
        ul_time  = time.time() - ul_start
        ul_speed = file_size / ul_time if ul_time > 0 else 0

        # ── SUCCESS ──
        if yt_link:
            await db.save_video(
                title=title, caption=caption, yt_link=yt_link, yt_id=yt_id,
                size_mb=size_mb,
                user_id=message.from_user.id if message.from_user else 0,
                username=(message.from_user.username or "") if message.from_user else "",
                file_unique_id=file_unique_id,
            )
            if chat_id in chat_progress:
                chat_progress[chat_id]["done"] += 1

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

        # ── QUOTA EXCEEDED ──
        elif status == "quota_exceeded":
            # Pehle is video ko pending mein save karo
            await db.save_pending_upload(
                chat_id=chat_id, message_id=message.id, title=title, caption=caption,
                file_size=file_size, size_mb=size_mb,
                user_id=message.from_user.id if message.from_user else 0,
                username=(message.from_user.username or "") if message.from_user else "",
                file_unique_id=file_unique_id,
            )
            if chat_id in chat_progress:
                chat_progress[chat_id]["failed"] += 1

            now_ist   = datetime.now(IST)
            retry_ist = now_ist.replace(hour=14, minute=0, second=0, microsecond=0)
            if now_ist >= retry_ist:
                retry_ist += timedelta(days=1)
            hours_left = (retry_ist - now_ist).total_seconds() / 3600

            await status_msg.edit_text(
                f"⚠️ **YouTube Quota Khatam!**\n\n"
                f"📌 `{title}`\n"
                f"💾 Pending mein save kar liya!\n\n"
                f"🕑 **Auto retry:** Kal 2:00 PM IST ({hours_left:.1f}h baad)\n"
                f"▶️ **Manual retry:** `/retry`\n\n"
                f"_Queue mein baaki sab bhi rok diye gaye hain._"
            )

            # Baki queue bhi rok do
            await handle_quota_exceeded()

        # ── OTHER ERROR ──
        else:
            await db.save_pending_upload(
                chat_id=chat_id, message_id=message.id, title=title, caption=caption,
                file_size=file_size, size_mb=size_mb,
                user_id=message.from_user.id if message.from_user else 0,
                username=(message.from_user.username or "") if message.from_user else "",
                file_unique_id=file_unique_id,
            )
            if chat_id in chat_progress:
                chat_progress[chat_id]["failed"] += 1

            await status_msg.edit_text(
                f"❌ **Upload Failed**\n\n"
                f"📌 `{title}`\n"
                f"💾 Pending mein save — `/retry` pe dobara hoga."
            )

    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.error(f"process_upload error: {e}", exc_info=True)
        if chat_id in chat_progress:
            chat_progress[chat_id]["failed"] += 1
        try:
            await status_msg.edit_text(f"❌ Error: `{str(e)[:300]}`")
        except Exception:
            pass
    finally:
        active_uploads.pop(file_unique_id, None)

        # Cleanup files
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except Exception:
                pass
        shutil.rmtree(tmp_dir, ignore_errors=True)

        # Progress pin update
        await update_progress_pin(chat_id)

        # Agar queue khatam ho gayi is chat ke liye
        if chat_id in chat_progress:
            info      = chat_progress[chat_id]
            completed = info["done"] + info["failed"]
            if completed >= info["total"]:
                await finish_progress_pin(chat_id)

# ══════════════════════════════════════════════════════════
# UPLOAD WORKERS
# ══════════════════════════════════════════════════════════

async def upload_worker(worker_id: int):
    logger.info(f"Upload Worker #{worker_id} ready")
    while True:
        try:
            task = await upload_queue.get()
            client, message, status_msg, file_unique_id, title, caption, file_size, size_mb = task
            try:
                await process_upload(client, message, status_msg,
                                     file_unique_id, title, caption, file_size, size_mb)
            except Exception as e:
                logger.error(f"Worker #{worker_id} error: {e}", exc_info=True)
            finally:
                upload_queue.task_done()
        except Exception as e:
            logger.error(f"Worker #{worker_id} fatal: {e}", exc_info=True)
            await asyncio.sleep(1)

# ══════════════════════════════════════════════════════════
# VIDEO HANDLER
# ══════════════════════════════════════════════════════════

@app.on_message(filters.video | filters.document)
async def handle_video(client, message: Message):
    await register_user(message)
    file = message.video or message.document
    if not file:
        return

    user_id = message.from_user.id if message.from_user else 0

    # ── CHECK MODE ─────────────────────────────────────────
    if user_id in check_mode:
        caption        = message.caption or ""
        file_unique_id = getattr(file, "file_unique_id", "") or str(message.id)
        dup = await db.is_duplicate(file_unique_id, caption)
        check_mode[user_id]["results"].append((
            dup is not None,
            caption or "(No caption)",
            dup["yt_link"] if dup else None,
        ))
        status = "✅ Already uploaded" if dup else "❌ Not uploaded"
        await message.reply_text(
            f"{status}\n`{(caption or '(No caption)')[:80]}`",
            quote=True,
        )
        return

    # ── DOCUMENT TYPE CHECK ────────────────────────────────
    if message.document:
        mime  = getattr(file, "mime_type", "") or ""
        fname = getattr(file, "file_name",  "") or ""
        ext   = os.path.splitext(fname)[1].lower()
        if mime not in SUPPORTED_MIME and ext not in SUPPORTED_EXT:
            await message.reply_text("❌ Sirf video files bhejo (MP4, MKV, etc.)")
            return

    # ── ACCOUNTS CHECK ─────────────────────────────────────
    if youtube.get_account_count() == 0:
        await message.reply_text(
            "❌ Koi YouTube account connected nahi hai!\n"
            "Owner ko `/addaccount ACC1` use karna hoga."
        )
        return

    # ── STOP / QUOTA CHECK ─────────────────────────────────
    if stop_flag:
        await message.reply_text(
            "🚫 **Uploads abhi band hain.**\n"
            "Admin ke `/resume` command ka wait karo."
        )
        return

    if quota_exceeded:
        pending_count = await db.get_pending_count()
        now_ist   = datetime.now(IST)
        retry_ist = now_ist.replace(hour=14, minute=0, second=0, microsecond=0)
        if now_ist >= retry_ist:
            retry_ist += timedelta(days=1)
        hours_left = (retry_ist - now_ist).total_seconds() / 3600
        await message.reply_text(
            f"⚠️ **YouTube Quota Khatam Hai**\n\n"
            f"📋 Abhi `{pending_count}` videos pending hain.\n"
            f"🕑 Auto retry: **2:00 PM IST** ({hours_left:.1f}h baad)\n"
            f"▶️ Ya `/retry` karo\n\n"
            f"_Abhi video mat bhejo — quota khatam hai._"
        )
        return

    file_unique_id = getattr(file, "file_unique_id", None) or str(message.id)
    caption        = message.caption or ""
    file_size      = getattr(file, "file_size", 0) or 0
    size_mb        = file_size / (1024 * 1024)

    # ── DUPLICATE CHECK (caption + file_unique_id) ─────────
    dup = await db.is_duplicate(file_unique_id, caption)
    if dup:
        await message.reply_text(
            f"⚠️ **Yeh video pehle hi upload ho chuki hai!**\n\n"
            f"📌 Title: `{dup.get('title', 'N/A')}`\n"
            f"🔗 **Link:** {dup['yt_link']}\n\n"
            f"_(Caption ya file same hai — dobara upload nahi hoga)_"
        )
        return

    # ── ALREADY IN QUEUE? ─────────────────────────────────
    if file_unique_id in active_uploads:
        await message.reply_text(
            "⚠️ **Yeh video already queue mein hai!**\n"
            "Pehle wali upload complete hone ka wait karo."
        )
        return

    # ── PARSE TITLE FROM CAPTION ──────────────────────────
    title = ""
    if caption:
        for line in caption.splitlines():
            if line.lower().startswith("file title"):
                parts = line.split(":", 1)
                if len(parts) == 2:
                    title = parts[1].strip()
                    break
    if not title:
        fname = getattr(file, "file_name", "") or ""
        title = os.path.splitext(fname)[0] if fname else f"Video_{message.id}"

    # ── QUEUE NUMBERING (per chat) ─────────────────────────
    chat_id = message.chat.id
    if chat_id not in chat_progress:
        chat_progress[chat_id] = {
            "total": 0, "done": 0, "failed": 0,
            "current": "", "pinned_msg_id": None,
        }
    chat_progress[chat_id]["total"] += 1
    queue_pos = chat_progress[chat_id]["total"]

    # Queue size info
    q_size = upload_queue.qsize()
    active_count = len(active_uploads)
    wait_est = max(0, q_size + active_count - UPLOAD_WORKERS)

    status_msg = await message.reply_text(
        f"⏳ **Queue mein add kiya — #{queue_pos}**\n\n"
        f"📌 `{title}`\n"
        f"📦 `{size_mb:.1f} MB`\n\n"
        f"📋 Queue size: `{q_size + 1}`\n"
        f"🔄 Processing: `{min(active_count, UPLOAD_WORKERS)}`\n"
        f"⏱ Approx wait: `{wait_est * 3}–{(wait_est+1) * 5} min` (estimate)"
    )

    active_uploads[file_unique_id] = True
    await upload_queue.put((
        client, message, status_msg, file_unique_id,
        title, caption, file_size, size_mb,
    ))

    # Progress pin update (sirf bulk ke liye)
    await update_progress_pin(chat_id)

# ══════════════════════════════════════════════════════════
# CHECK MODE COMMANDS
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("check"))
async def check_cmd(client, message: Message):
    await register_user(message)
    user_id = message.from_user.id
    check_mode[user_id] = {"results": []}
    await message.reply_text(
        "**🔍 Check Mode ON**\n\n"
        "Ab jo bhi videos forward karo, main check karunga:\n"
        "✅ Already uploaded hai ya ❌ nahi\n\n"
        "Jab sab forward kar lo toh `/checkdone` bhejo\n"
        "_Check mode mein upload nahi hoga, sirf check hoga._"
    )

@app.on_message(filters.command("checkdone"))
async def check_done_cmd(client, message: Message):
    user_id = message.from_user.id
    if user_id not in check_mode:
        await message.reply_text("❌ Check mode ON nahi hai. Pehle `/check` use karo.")
        return

    data    = check_mode.pop(user_id)
    results = data["results"]   # [(ok:bool, caption:str, yt_link:str|None)]

    if not results:
        await message.reply_text("Koi video check nahi hua. Mode band kar diya.")
        return

    uploaded     = [(i+1, cap, link) for i,(ok,cap,link) in enumerate(results) if ok]
    not_uploaded = [(i+1, cap)       for i,(ok,cap,link) in enumerate(results) if not ok]

    summary = (
        f"**📋 Check Results — {len(results)} videos**\n\n"
        f"✅ Uploaded     : `{len(uploaded)}`\n"
        f"❌ Not Uploaded : `{len(not_uploaded)}`\n"
    )
    await message.reply_text(summary)

    # Not uploaded ki numbered list alag message mein
    if not_uploaded:
        lines = ["**❌ Ye upload nahi hue (in captions ko upload karo):**\n"]
        for num, cap in not_uploaded:
            short = cap[:100].replace("\n", " ")
            lines.append(f"{num}. `{short}`")

        # Telegram 4096 char limit — split karo agar zyada ho
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 4000:
                await message.reply_text(chunk)
                chunk = line + "\n"
            else:
                chunk += line + "\n"
        if chunk:
            await message.reply_text(chunk)

    # Uploaded ki list (optional — short)
    if uploaded:
        lines = ["**✅ Ye already uploaded hain:**\n"]
        for num, cap, link in uploaded:
            short = cap[:60].replace("\n", " ")
            lines.append(f"{num}. {short} — {link}")
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 4000:
                await message.reply_text(chunk)
                chunk = line + "\n"
            else:
                chunk += line + "\n"
        if chunk:
            await message.reply_text(chunk)

# ══════════════════════════════════════════════════════════
# RETRY COMMAND
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("retry"))
async def retry_cmd(client, message: Message):
    await register_user(message)
    user_id = message.from_user.id
    # Sirf owner ya pending wala user
    if user_id != OWNER_ID:
        pending_count = await db.get_pending_count()
        if pending_count == 0:
            await message.reply_text("✅ Koi pending upload nahi hai.")
            return
        await message.reply_text(
            f"📋 `{pending_count}` videos pending hain.\n"
            f"Admin retry karega ya 2 PM IST pe auto hoga."
        )
        return

    pending_count = await db.get_pending_count()
    if pending_count == 0:
        await message.reply_text("✅ Koi pending upload nahi hai.")
        return

    await message.reply_text(
        f"🔄 **Manual Retry Shuru**\n\n"
        f"📋 `{pending_count}` videos queue mein daal rahe hain...\n"
        f"_(Quota check hoga — agar abhi bhi khatam hai to dobara pending ho jaayenge)_"
    )
    await retry_pending_uploads(triggered_by=user_id)

# ══════════════════════════════════════════════════════════
# PENDING COMMAND
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("pending") & filters.user(OWNER_ID))
async def pending_cmd(client, message: Message):
    docs  = await db.get_pending_uploads()
    count = len(docs)
    if count == 0:
        await message.reply_text("✅ Koi pending upload nahi hai.")
        return

    now_ist   = datetime.now(IST)
    retry_ist = now_ist.replace(hour=14, minute=0, second=0, microsecond=0)
    if now_ist >= retry_ist:
        retry_ist += timedelta(days=1)
    hours_left = (retry_ist - now_ist).total_seconds() / 3600

    text = f"**📋 Pending Uploads ({count})**\n"
    text += f"🕑 Auto retry: **2 PM IST** ({hours_left:.1f}h baad)\n\n"
    for i, d in enumerate(docs[:20], 1):
        text += f"{i}. `{d['title'][:50]}` — `{d['size_mb']:.1f} MB`\n"
    if count > 20:
        text += f"\n_...aur {count - 20} aur..._"

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Retry Abhi", callback_data="admin_retry_now"),
        InlineKeyboardButton("🗑 Clear All",  callback_data="admin_clear_pending"),
    ]])
    await message.reply_text(text, reply_markup=kb)

@app.on_callback_query(filters.regex("^admin_retry_now$") & filters.user(OWNER_ID))
async def cb_retry_now(client, callback: CallbackQuery):
    await callback.answer("Retry shuru ho raha hai...", show_alert=False)
    await callback.message.edit_text("🔄 Retry shuru ho raha hai...")
    await retry_pending_uploads(triggered_by=OWNER_ID)

@app.on_callback_query(filters.regex("^admin_clear_pending$") & filters.user(OWNER_ID))
async def cb_clear_pending(client, callback: CallbackQuery):
    deleted = await db.clear_all_pending()
    await callback.answer(f"✅ {deleted} pending clear ho gaye.", show_alert=True)
    await callback.message.edit_text(f"🗑 `{deleted}` pending uploads clear kar diye.")

# ══════════════════════════════════════════════════════════
# STOP / RESUME / CLEARQUOTA (OWNER ONLY)
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("stop") & filters.user(OWNER_ID))
async def stop_cmd(client, message: Message):
    global stop_flag
    stop_flag = True
    await message.reply_text(
        "🛑 **Uploads band kar diye gaye.**\n"
        "Queue mein jo bhi hai wo pending mein save ho jaayega.\n"
        "Dobara chalu karne ke liye `/resume` karo."
    )

@app.on_message(filters.command("resume") & filters.user(OWNER_ID))
async def resume_cmd(client, message: Message):
    global stop_flag
    stop_flag = False
    await message.reply_text(
        "▶️ **Uploads resume ho gaye!**\n\n"
        "Pending uploads ke liye `/retry` karo."
    )

@app.on_message(filters.command("clearquota") & filters.user(OWNER_ID))
async def clearquota_cmd(client, message: Message):
    global quota_exceeded
    quota_exceeded = False
    await db.set_setting("quota_exceeded", False)
    await message.reply_text(
        "✅ **Quota flag reset ho gaya!**\n\n"
        "Ab naye uploads ho sakenge.\n"
        "Pending ke liye `/retry` karo."
    )

# ══════════════════════════════════════════════════════════
# YOUTUBE ACCOUNT MANAGEMENT
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("addaccount"))
async def add_account_cmd(client, message: Message):
    await register_user(message)
    user_id    = message.from_user.id if message.from_user else 0
    is_owner   = (user_id == OWNER_ID)
    is_premium = await db.is_premium_user(user_id)
    if not is_owner and not is_premium:
        await message.reply_text(
            "**❌ Sirf Premium Users hi YouTube account add kar sakte hain!**\n\n"
            "Premium lene ke liye neeche tap karo 👇",
            reply_markup=contact_keyboard(),
        )
        return

    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text("**Usage:** `/addaccount ACCOUNT_NAME`")
        return

    acc_name = parts[1].strip().replace(" ", "_")
    if acc_name in youtube.accounts:
        await message.reply_text(f"⚠️ Account `{acc_name}` already exist karta hai!")
        return

    auth_url = youtube.start_auth(acc_name)
    if auth_url:
        pending_add[user_id] = acc_name
        await message.reply_text(
            f"**🔐 Account `{acc_name}` Add karo**\n\n"
            f"**Step 1:** Iss link pe jao:\n`{auth_url}`\n\n"
            f"**Step 2:** Google account se login karo\n"
            f"**Step 3:** Allow karo → Code copy karo\n"
            f"**Step 4:** Bot ko bhejo:\n"
            f"`/code {acc_name} YAHAN_CODE_DAALO`\n\n"
            f"⏰ Code 2 minute mein expire hota hai!"
        )
    else:
        await message.reply_text("❌ Auth URL generate nahi hua. Dobara try karo.")

@app.on_message(filters.command("code"))
async def code_cmd(client, message: Message):
    await register_user(message)
    user_id    = message.from_user.id if message.from_user else 0
    is_owner   = (user_id == OWNER_ID)
    is_premium = await db.is_premium_user(user_id)
    if not is_owner and not is_premium:
        await message.reply_text("❌ Sirf premium users hi account add kar sakte hain.")
        return

    parts = message.text.split(None, 2)
    if len(parts) < 3:
        await message.reply_text("**Usage:** `/code ACCOUNT_NAME AUTH_CODE`")
        return

    acc_name = parts[1].strip()
    code     = parts[2].strip()
    await message.reply_text(f"⏳ Account `{acc_name}` authorize ho raha hai...")

    success = youtube.finish_auth(acc_name, code)
    if success:
        # Token MongoDB mein save karo (Railway restart safe)
        try:
            token_data = youtube.get_token_data(acc_name)
            await db.save_yt_token(acc_name, token_data)
        except Exception as e:
            logger.warning(f"Token save error: {e}")

        await message.reply_text(
            f"✅ **Account `{acc_name}` Successfully Added!**\n\n"
            f"Total accounts: `{youtube.get_account_count()}`\n"
            f"💾 Token MongoDB mein save — restart safe ✅\n\n"
            f"{youtube.get_accounts_status()}"
        )
    else:
        await message.reply_text(
            f"❌ **Authorization Failed!**\n\n"
            f"Code galat ya expire ho gaya.\n"
            f"Dobara `/addaccount {acc_name}` karo aur jaldi code bhejo."
        )

@app.on_message(filters.command("accounts"))
async def accounts_cmd(client, message: Message):
    await register_user(message)
    count     = youtube.get_account_count()
    status    = youtube.get_accounts_status()
    daily_lim = count * 6
    await message.reply_text(
        f"**📋 YouTube Accounts**\n\n"
        f"{status}\n\n"
        f"**Total:** `{count}` accounts\n"
        f"**Daily Limit:** ~`{daily_lim}` videos/day"
    )

# ══════════════════════════════════════════════════════════
# STATS COMMANDS
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("links"))
async def links_cmd(client, message: Message):
    await register_user(message)
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
    await register_user(message)
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text("Usage: `/search VIDEO TITLE`")
        return
    query  = parts[1].strip()
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
    await register_user(message)
    total        = await db.get_total_count()
    total_size   = await db.get_total_size()
    acc_count    = youtube.get_account_count()
    total_users  = await db.get_total_users()
    premium_list = await db.get_premium_users()
    pending_cnt  = await db.get_pending_count()
    quota_flag   = await db.get_setting("quota_exceeded", False)

    await message.reply_text(
        f"**📊 Bot Stats**\n\n"
        f"🎬 Total Videos  : `{total}`\n"
        f"💾 Total Size    : `{total_size:.1f} MB`\n"
        f"👤 YT Accounts   : `{acc_count}`\n"
        f"📈 Daily Capacity: ~`{acc_count * 6}` videos\n"
        f"👥 Total Users   : `{total_users}`\n"
        f"⭐ Premium Users : `{len(premium_list)}`\n"
        f"⏳ Pending       : `{pending_cnt}`\n"
        f"⚠️ Quota Status  : {'❌ Exceeded' if quota_flag else '✅ OK'}"
    )

@app.on_message(filters.command("botstats"))
async def botstats_cmd(client, message: Message):
    await register_user(message)
    now         = datetime.utcnow()
    today       = now.replace(hour=0, minute=0, second=0, microsecond=0)
    this_week   = now - timedelta(days=7)
    this_month  = now - timedelta(days=30)

    total_users    = await db.get_total_users()
    daily_new      = await db.get_users_since(today)
    weekly_new     = await db.get_users_since(this_week)
    monthly_new    = await db.get_users_since(this_month)
    daily_active   = await db.get_active_users_since(today)
    weekly_active  = await db.get_active_users_since(this_week)
    monthly_active = await db.get_active_users_since(this_month)
    daily_vids     = await db.get_videos_since(today)
    weekly_vids    = await db.get_videos_since(this_week)
    monthly_vids   = await db.get_videos_since(this_month)
    total_vids     = await db.get_total_count()
    premium_count  = len(await db.get_premium_users())

    await message.reply_text(
        f"**📈 Bot Detailed Stats**\n\n"
        f"**👥 New Users Joined:**\n"
        f"• Aaj: `{daily_new}`\n"
        f"• Is Hafte: `{weekly_new}`\n"
        f"• Is Mahine: `{monthly_new}`\n"
        f"• Overall: `{total_users}`\n\n"
        f"**🟢 Active Users:**\n"
        f"• Aaj: `{daily_active}`\n"
        f"• Is Hafte: `{weekly_active}`\n"
        f"• Is Mahine: `{monthly_active}`\n\n"
        f"**🎬 Videos Uploaded:**\n"
        f"• Aaj: `{daily_vids}`\n"
        f"• Is Hafte: `{weekly_vids}`\n"
        f"• Is Mahine: `{monthly_vids}`\n"
        f"• Overall: `{total_vids}`\n\n"
        f"**⭐ Premium Users: `{premium_count}`**"
    )

# ══════════════════════════════════════════════════════════
# CONTACT / SECRET CHAT
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("contact"))
async def contact_cmd(client, message: Message):
    await register_user(message)
    await message.reply_text(
        "**📩 Admin se contact karo**\n\nNeeche se option chunno 👇",
        reply_markup=contact_keyboard(),
    )

async def _forward_to_admin(client, user, user_msg: str, msg_type: str):
    username   = f"@{user.username}" if user.username else "N/A"
    type_label = "⭐ Premium Request" if msg_type == "premium" else "💬 Message"
    try:
        sent = await client.send_message(
            OWNER_ID,
            f"**📩 {type_label}**\n\n"
            f"👤 **User:** {user.first_name}\n"
            f"🔗 **Username:** {username}\n"
            f"🆔 **ID:** `{user.id}`\n\n"
            f"**💬 Message:**\n{user_msg}\n\n"
            f"_Reply karo iss message pe — seedha user tak pahunch jayega!_\n"
            f"_Ya:_ `/reply {user.id} Jawab`"
        )
        contact_reply_map[sent.id] = user.id
        return True
    except Exception as e:
        logger.error(f"Forward error: {e}")
        return False

@app.on_callback_query(filters.regex("^contact_premium$"))
async def cb_contact_premium(client, callback: CallbackQuery):
    user_id = callback.from_user.id
    contact_state[user_id] = "premium"
    await callback.message.edit_text(
        "**⭐ Premium Request**\n\n"
        "Apni **Gmail ID** bhejo jis se aap YouTube channel chalate ho.\n\n"
        "**Example:** `yourname@gmail.com`\n\n"
        "_Bas email type karo aur send karo_ 👇"
    )
    await callback.answer()

@app.on_callback_query(filters.regex("^contact_message$"))
async def cb_contact_message(client, callback: CallbackQuery):
    user_id = callback.from_user.id
    contact_state[user_id] = "message"
    await callback.message.edit_text(
        "**💬 Admin ko Message**\n\nApna message type karo aur send karo 👇\n\n"
        "_Admin jald reply karenge._"
    )
    await callback.answer()

@app.on_callback_query(filters.regex("^my_status$"))
async def cb_my_status(client, callback: CallbackQuery):
    user_id    = callback.from_user.id
    is_premium = await db.is_premium_user(user_id)
    badge      = "⭐ Premium" if is_premium else "👤 Free"
    await callback.answer(f"Aapka status: {badge}", show_alert=True)

@app.on_callback_query(filters.regex("^remove_premium_req$"))
async def cb_remove_premium_req(client, callback: CallbackQuery):
    user     = callback.from_user
    username = f"@{user.username}" if user.username else "N/A"
    try:
        await client.send_message(
            OWNER_ID,
            f"**❌ Premium Remove Request**\n\n"
            f"👤 **User:** {user.first_name}\n"
            f"🔗 **Username:** {username}\n"
            f"🆔 **ID:** `{user.id}`\n\n"
            f"`/removepremium {user.id}`"
        )
        await callback.answer("✅ Request bhej di gayi!", show_alert=True)
    except Exception:
        await callback.answer("❌ Request nahi gayi, dobara try karo.", show_alert=True)

# Text handler for contact flow (non-command user messages)
_ignored_commands = [
    "start","help","mypremium","contact","addaccount","code","accounts",
    "links","search","stats","botstats","reply","broadcast","addpremium",
    "removepremium","premiumlist","check","checkdone","retry","pending",
    "stop","resume","clearquota",
]

@app.on_message(
    filters.text
    & ~filters.command(_ignored_commands)
    & ~filters.user(OWNER_ID)
)
async def handle_contact_reply(client, message: Message):
    await register_user(message)
    user_id = message.from_user.id if message.from_user else 0
    state   = contact_state.pop(user_id, None)
    if not state:
        return

    user      = message.from_user
    user_text = message.text.strip()
    full_msg  = f"📧 Email: {user_text}\n\nMujhe premium chahiye." if state == "premium" else user_text

    success = await _forward_to_admin(client, user, full_msg, state)
    if success:
        reply = (
            "**✅ Premium Request Admin ko pahunch gayi!**\n\n"
            "Admin aapki Gmail ID verify karke premium add kar denge. 🎉"
            if state == "premium"
            else "**✅ Message admin ko pahunch gaya!**\n\nAdmin jald reply karenge."
        )
    else:
        reply = "❌ Message nahi bheja ja saka.\n" + (ADMIN_LINK if ADMIN_LINK else "")
    await message.reply_text(reply)

# ══════════════════════════════════════════════════════════
# ADMIN REPLY TO USER
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("reply") & filters.user(OWNER_ID))
async def reply_user_cmd(client, message: Message):
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        await message.reply_text("**Usage:** `/reply USER_ID Aapka jawab`")
        return
    try:
        target_id = int(parts[1].strip())
    except ValueError:
        await message.reply_text("❌ Valid User ID daalo.")
        return
    reply_text = parts[2].strip()
    try:
        await client.send_message(target_id, f"**📩 Admin ka jawab:**\n\n{reply_text}")
        await message.reply_text(f"✅ Reply `{target_id}` ko bhej di!")
    except Exception as e:
        await message.reply_text(f"❌ Reply nahi bhej saka: `{e}`")

@app.on_message(
    filters.user(OWNER_ID) & filters.reply
    & ~filters.command(_ignored_commands)
)
async def admin_native_reply(client, message: Message):
    replied = message.reply_to_message
    if not replied:
        return
    target_user_id = contact_reply_map.get(replied.id)
    if not target_user_id:
        return
    reply_text = message.text or message.caption or ""
    if not reply_text:
        return
    try:
        await client.send_message(target_user_id, f"**📩 Admin ka jawab:**\n\n{reply_text}")
        await message.reply_text(f"✅ Reply user `{target_user_id}` ko deliver ho gayi!")
    except Exception as e:
        await message.reply_text(f"❌ Deliver nahi hui: `{e}`")

# ══════════════════════════════════════════════════════════
# BROADCAST
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def broadcast_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        total = await db.get_total_users()
        await message.reply_text(
            f"**Usage:** `/broadcast Aapka message`\n\n📊 Total users: `{total}`"
        )
        return
    broadcast_text = parts[1].strip()
    user_ids       = await db.get_all_user_ids()
    status_msg     = await message.reply_text(
        f"📡 **Broadcast shuru...**\n\nTotal: `{len(user_ids)}`"
    )
    success = failed = 0
    for uid in user_ids:
        try:
            await client.send_message(uid, f"📢 **Admin ka message:**\n\n{broadcast_text}")
            success += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await status_msg.edit_text(
        f"**✅ Broadcast Complete!**\n\n"
        f"✅ Bheja: `{success}`\n❌ Failed: `{failed}`\n📊 Total: `{len(user_ids)}`"
    )

# ══════════════════════════════════════════════════════════
# PREMIUM MANAGEMENT
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("addpremium") & filters.user(OWNER_ID))
async def add_premium_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text("**Usage:** `/addpremium USER_ID`")
        return
    try:
        target_id = int(parts[1].strip())
    except ValueError:
        await message.reply_text("❌ Valid Telegram User ID daalo.")
        return
    await db.add_premium_user(target_id)
    await message.reply_text(f"✅ **User `{target_id}` ko Premium mil gaya!**")
    try:
        await client.send_message(
            target_id,
            "**🎉 Congratulations! Aapko Premium mil gaya!**\n\n"
            "⭐ Ab aap `/addaccount` se apna YouTube channel add kar sakte ho!"
        )
    except Exception:
        pass

@app.on_message(filters.command("removepremium") & filters.user(OWNER_ID))
async def remove_premium_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text("**Usage:** `/removepremium USER_ID`")
        return
    try:
        target_id = int(parts[1].strip())
    except ValueError:
        await message.reply_text("❌ Valid User ID daalo.")
        return
    success = await db.remove_premium_user(target_id)
    if success:
        await message.reply_text(f"✅ User `{target_id}` ka premium remove ho gaya.")
    else:
        await message.reply_text(f"❌ User `{target_id}` premium list mein nahi tha.")

@app.on_message(filters.command("premiumlist") & filters.user(OWNER_ID))
async def premium_list_cmd(client, message: Message):
    users = await db.get_premium_users()
    if not users:
        await message.reply_text("❌ Koi premium user nahi hai.")
        return
    text = f"**⭐ Premium Users ({len(users)}):**\n\n"
    for u in users:
        uname = f"@{u.get('username','')}" if u.get("username") else "N/A"
        text += f"• `{u['user_id']}` — {uname}\n"
    await message.reply_text(text)

# ══════════════════════════════════════════════════════════
# START COMMAND
# ══════════════════════════════════════════════════════════

@app.on_message(filters.command("start"))
async def start_cmd(client, message: Message):
    try:
        await register_user(message)
    except Exception:
        pass
    user_id    = message.from_user.id if message.from_user else 0
    is_premium = await db.is_premium_user(user_id)
    badge      = "⭐ Premium" if is_premium else "👤 Free"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ Premium Lena Hai (Free!)", callback_data="contact_premium")],
        [InlineKeyboardButton("📊 My Status", callback_data="my_status")],
    ])
    await message.reply_text(
        f"**🎬 YouTube Auto Uploader Bot**\n\n"
        f"Video bhejo → YouTube pe upload → Link milega!\n\n"
        f"**Formats:** MP4, MKV, WebM, AVI, MOV\n"
        f"**Mode:** Unlisted 🔒  |  **Multi-Account:** Auto-rotate ✅\n\n"
        f"**Aapka Status:** {badge}\n\n"
        f"/check — Videos upload hain ya nahi check karo\n"
        f"/contact — Premium lo ya kuch poochho\n"
        f"/links — Recent uploads",
        reply_markup=kb,
    )

@app.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    await register_user(message)
    await message.reply_text(
        "**📖 Commands**\n\n"
        "**📹 Upload**\nBas video bhejo — auto upload hoga!\n\n"
        "**🔍 Check Mode**\n"
        "/check — Mode ON karo\n"
        "/checkdone — Result dekho (numbered)\n\n"
        "**📊 Data**\n"
        "/links — Last 10 uploads\n"
        "/search TITLE — Video dhundho\n"
        "/stats — Total stats\n\n"
        "**⏳ Pending**\n"
        "/retry — Pending uploads manually retry karo\n"
        "_(Auto 2 PM IST pe bhi hota hai)_\n\n"
        "**⭐ Premium (FREE!)**\n/contact → Email bhejo → Premium milega!",
        reply_markup=contact_keyboard(),
    )

@app.on_message(filters.command("mypremium"))
async def mypremium_cmd(client, message: Message):
    await register_user(message)
    user_id    = message.from_user.id if message.from_user else 0
    is_premium = await db.is_premium_user(user_id)
    if is_premium:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Premium Remove Karna Hai", callback_data="remove_premium_req")]
        ])
        await message.reply_text(
            "**⭐ Aap Premium User hain!**\n\n"
            "✅ `/addaccount` se apna YouTube channel add kar sakte ho",
            reply_markup=kb,
        )
    else:
        await message.reply_text(
            "**👤 Aap Free User hain**\n\n"
            "⭐ **Premium bilkul FREE hai!**\n"
            "Neeche tap karo 👇",
            reply_markup=contact_keyboard(),
        )

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

async def main():
    global quota_exceeded

    # Health server thread
    Thread(target=start_health_server, daemon=True).start()

    # Bot commands menu set karo
    set_bot_commands_via_api()

    await app.start()
    logger.info("✅ Bot start ho gaya!")

    # DB se quota flag restore karo (restart safe)
    quota_exceeded = await db.get_setting("quota_exceeded", False)
    if quota_exceeded:
        logger.warning("⚠️ Quota exceeded flag DB se restore hua")

    # Load YT tokens from MongoDB (Railway restart safe)
    try:
        all_tokens = await db.get_all_yt_tokens()
        for tok in all_tokens:
            youtube.load_token(tok["account_name"], tok["token"])
        logger.info(f"✅ {len(all_tokens)} YT accounts MongoDB se load kiye")
    except Exception as e:
        logger.error(f"Token load error: {e}")

    # Upload workers launch karo
    for i in range(UPLOAD_WORKERS):
        asyncio.create_task(upload_worker(i + 1))

    # Auto retry scheduler (2 PM IST daily)
    asyncio.create_task(auto_retry_scheduler())

    logger.info(f"✅ {UPLOAD_WORKERS} upload workers + auto-retry scheduler ready")
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
