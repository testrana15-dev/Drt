import os
import asyncio
import logging
import time
import tempfile
import shutil
import json
import urllib.request
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters, idle
from pyrogram.types import Message
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
ADMIN_LINK  = os.environ.get("ADMIN_LINK", "")
# =======================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def set_bot_commands_via_api():
    commands = [
        {"command": "start",         "description": "Bot shuru karo"},
        {"command": "help",          "description": "Sare commands dekho"},
        {"command": "accounts",      "description": "Connected YT accounts dekho"},
        {"command": "addaccount",    "description": "Apna YouTube account add karo"},
        {"command": "removeaccount", "description": "YouTube account remove karo"},
        {"command": "code",          "description": "Auth code submit karo"},
        {"command": "links",         "description": "Last 10 uploaded videos"},
        {"command": "search",        "description": "Video title se search karo"},
        {"command": "stats",         "description": "Bot ki total statistics"},
        {"command": "mypremium",     "description": "Apna premium status dekho"},
    ]
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands"
    data = json.dumps({"commands": commands}).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        resp = urllib.request.urlopen(req)
        result = json.loads(resp.read())
        if result.get("ok"):
            logger.info("✅ Commands menu set ho gaya!")
        else:
            logger.warning(f"Commands set failed: {result}")
    except Exception as e:
        logger.error(f"Commands API error: {e}")


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

# Duplicate upload prevention: file_unique_id -> True
active_uploads = {}


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
    user_id = message.from_user.id if message.from_user else 0
    is_premium = await db.is_premium_user(user_id)
    premium_badge = "⭐ Premium" if is_premium else "👤 Free"

    await message.reply_text(
        f"**🎬 YouTube Auto Uploader Bot**\n\n"
        f"Video bhejo → YouTube pe upload → Link milega!\n\n"
        f"**Formats:** MP4, MKV, WebM, AVI, MOV\n"
        f"**Mode:** Unlisted 🔒\n"
        f"**Multi-Account:** Auto-rotate ✅\n\n"
        f"Caption = YouTube title 📌\n\n"
        f"**Aapka Status:** {premium_badge}\n\n"
        f"/accounts — Connected accounts dekho\n"
        f"/links — Recent uploads\n"
        f"/stats — Upload statistics\n"
        f"/mypremium — Premium status"
    )


@app.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    admin_text = f"\n📩 Admin se contact karo: {ADMIN_LINK}" if ADMIN_LINK else ""
    await message.reply_text(
        "**📖 Commands**\n\n"
        "**📹 Upload**\n"
        "Bas video bhejo — auto upload hoga!\n\n"
        "**📊 Data Commands**\n"
        "/links — Last 10 uploads\n"
        "/search TITLE — Video dhundho\n"
        "/stats — Total stats\n\n"
        "**🎬 YouTube Account**\n"
        "/addaccount NAME — Apna YT account add karo (Premium)\n"
        "/removeaccount NAME — Apna account remove karo (Premium)\n"
        "/accounts — Sare accounts dekho\n"
        "/mypremium — Apna premium status\n\n"
        "**⭐ Premium kaise len?**\n"
        f"Premium users hi apna YouTube channel add kar sakte hain.{admin_text}"
    )


@app.on_message(filters.command("mypremium"))
async def mypremium_cmd(client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    is_premium = await db.is_premium_user(user_id)
    admin_text = f"\n\n📩 Premium ke liye admin se milo: {ADMIN_LINK}" if ADMIN_LINK else "\n\n📩 Admin se contact karo premium ke liye."

    if is_premium:
        await message.reply_text(
            "**⭐ Aap Premium User hain!**\n\n"
            "✅ Aap `/addaccount` use karke apna YouTube channel add kar sakte ho.\n"
            "✅ Unlimited video upload\n\n"
            "**YouTube Account Add karne ke steps:**\n"
            "1. `/addaccount APNA_NAAM` bhejo\n"
            "2. Auth link pe jao\n"
            "3. Google login karo\n"
            "4. Code copy karke `/code NAAM CODE` bhejo"
        )
    else:
        await message.reply_text(
            "**👤 Aap Free User hain**\n\n"
            "❌ Aap abhi apna YouTube channel add nahi kar sakte.\n\n"
            "**⭐ Premium ke liye zaroor hai:**\n"
            "• Aapki Gmail Google Cloud Console mein add honi chahiye\n"
            "• YouTube channel banana hoga (agar nahi hai)\n"
            "• Phone number verify hona chahiye\n\n"
            f"Premium milne ke baad aap apna channel add kar sakte ho.{admin_text}"
        )


# ══════════════════════════════════════
#        OWNER: PREMIUM MANAGEMENT
# ══════════════════════════════════════

@app.on_message(filters.command("addpremium") & filters.user(OWNER_ID))
async def add_premium_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text(
            "**Usage:** `/addpremium USER_ID`\n\n"
            "**Example:** `/addpremium 123456789`\n\n"
            "User ka Telegram ID daalo."
        )
        return

    try:
        target_id = int(parts[1].strip())
    except ValueError:
        await message.reply_text("❌ Valid Telegram User ID daalo (sirf numbers).")
        return

    await db.add_premium_user(target_id)
    await message.reply_text(
        f"✅ **User `{target_id}` ko Premium mil gaya!**\n\n"
        f"Ab wo `/addaccount` use kar sakta hai."
    )


@app.on_message(filters.command("removepremium") & filters.user(OWNER_ID))
async def remove_premium_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text("**Usage:** `/removepremium USER_ID`")
        return

    try:
        target_id = int(parts[1].strip())
    except ValueError:
        await message.reply_text("❌ Valid Telegram User ID daalo.")
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
        await message.reply_text("❌ Koi premium user nahi hai abhi.")
        return
    text = f"**⭐ Premium Users ({len(users)}):**\n\n"
    for u in users:
        uname = u.get("username", "")
        uname_str = f"@{uname}" if uname else "N/A"
        text += f"• `{u['user_id']}` — {uname_str}\n"
    await message.reply_text(text)


# ══════════════════════════════════════
#        ACCOUNT MANAGEMENT (PREMIUM)
# ══════════════════════════════════════

@app.on_message(filters.command("addaccount"))
async def add_account_cmd(client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    is_owner = (user_id == OWNER_ID)
    is_premium = await db.is_premium_user(user_id)

    if not is_owner and not is_premium:
        admin_text = f"\n\n📩 Premium ke liye yahan contact karo: {ADMIN_LINK}" if ADMIN_LINK else "\n\n📩 Admin se premium lene ke liye contact karo."
        await message.reply_text(
            "**❌ Sirf Premium Users hi YouTube account add kar sakte hain!**\n\n"
            "**⭐ Premium kaise milega?**\n"
            "• Admin aapka Telegram ID premium list mein add karega\n"
            "• Phir aap apna YouTube channel connect kar sakte ho\n"
            f"{admin_text}"
        )
        return

    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await message.reply_text(
            "**Usage:** `/addaccount ACCOUNT_NAME`\n\n"
            "**Example:** `/addaccount MeraChannel`"
        )
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
            f"**⚠️ Pehle yeh zaroori hai:**\n"
            f"1️⃣ Google account mein **phone number verify** hona chahiye\n"
            f"2️⃣ YouTube pe **apna channel** banana hoga (agar nahi hai)\n"
            f"3️⃣ **Aapki Gmail** Google Cloud Console mein test user ke roop mein add honi chahiye — warna upload fail hoga!\n\n"
            f"**Agar sab ready hai toh:**\n"
            f"**Step 1:** Iss link pe jao:\n`{auth_url}`\n\n"
            f"**Step 2:** Apne Google account se login karo\n"
            f"**Step 3:** Allow karo → Code copy karo\n"
            f"**Step 4:** Bot ko bhejo:\n"
            f"`/code {acc_name} YAHAN_CODE_DAALO`\n\n"
            f"⏰ Code 2 minute mein expire hota hai — jaldi karo!"
        )
    else:
        await message.reply_text("❌ Auth URL generate nahi hua. Dobara try karo.")


@app.on_message(filters.command("code"))
async def code_cmd(client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    is_owner = (user_id == OWNER_ID)
    is_premium = await db.is_premium_user(user_id)

    if not is_owner and not is_premium:
        await message.reply_text("❌ Sirf premium users hi account add kar sakte hain.")
        return

    parts = message.text.split(None, 2)
    if len(parts) < 3:
        await message.reply_text(
            "**Usage:** `/code ACCOUNT_NAME AUTH_CODE`\n\n"
            "**Example:** `/code MeraChannel 4/0AX9abc...`"
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
            f"Dobara `/addaccount {acc_name}` karo aur jaldi code bhejo.\n\n"
            f"**Agar baar baar fail ho raha hai:**\n"
            f"• Check karo ki aapki Gmail Google Cloud Console mein **test user** ke roop mein add hai\n"
            f"• YouTube channel bana lo agar nahi hai\n"
            f"• Phone number verify hona chahiye Google account mein"
        )


@app.on_message(filters.command("removeaccount"))
async def remove_account_cmd(client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    is_owner = (user_id == OWNER_ID)
    is_premium = await db.is_premium_user(user_id)

    if not is_owner and not is_premium:
        await message.reply_text("❌ Sirf premium users hi account remove kar sakte hain.")
        return

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

    # ── Duplicate upload prevention ──
    file_unique_id = getattr(file, 'file_unique_id', None) or str(message.id)
    if file_unique_id in active_uploads:
        await message.reply_text(
            "⚠️ **Yeh video already upload ho rahi hai!**\n\n"
            "Pehle wali upload complete hone ka wait karo."
        )
        return
    active_uploads[file_unique_id] = True
    # ────────────────────────────────

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

        yt_link, yt_id, status = await upload_future
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
        # Remove from active uploads
        active_uploads.pop(file_unique_id, None)
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except Exception:
                pass
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    set_bot_commands_via_api()
    Thread(target=start_health_server, daemon=True).start()
    logger.info("Bot starting...")
    app.run()
