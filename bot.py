import os
import asyncio
import logging
import time
import tempfile
import shutil
import json
import urllib.request
from datetime import datetime, timedelta
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
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
        {"command": "addaccount",    "description": "Apna YouTube account add karo (Premium)"},
        {"command": "code",          "description": "Auth code submit karo"},
        {"command": "links",         "description": "Last 10 uploaded videos"},
        {"command": "search",        "description": "Video title se search karo"},
        {"command": "stats",         "description": "Bot ki total statistics"},
        {"command": "botstats",      "description": "Daily/Weekly/Monthly user stats"},
        {"command": "mypremium",     "description": "Apna premium status dekho"},
        {"command": "contact",       "description": "Premium lo ya admin se baat karo"},
        {"command": "addpremium",    "description": "User ko premium do (Admin only)"},
        {"command": "removepremium", "description": "User ka premium hato (Admin only)"},
        {"command": "premiumlist",   "description": "Sare premium users dekho (Admin only)"},
        {"command": "broadcast",     "description": "Sab users ko message bhejo (Admin only)"},
        {"command": "reply",         "description": "User ko jawab bhejo (Admin only)"},
        {"command": "pending",       "description": "Pending uploads dekho (Admin only)"},
        {"command": "retrypending",  "description": "Pending uploads abhi retry karo (Admin only)"},
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

# Upload queue system
active_uploads = {}          # file_unique_id -> True (already queued/processing)
upload_queue = asyncio.Queue()
queue_positions = {}         # file_unique_id -> status_msg (for position updates)
UPLOAD_WORKERS = 2           # Ek saath max 2 videos process honge

# Secret chat: admin_message_id -> user_id mapping
contact_reply_map = {}

# Contact flow state: user_id -> "premium" | "message"
contact_state = {}


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


async def register_user(message: Message):
    if not message.from_user:
        return
    u = message.from_user
    await db.save_user(
        user_id=u.id,
        username=u.username or "",
        first_name=u.first_name or ""
    )


def contact_keyboard():
    buttons = [
        [InlineKeyboardButton("⭐ Premium Lena Hai (Free)", callback_data="contact_premium")],
        [InlineKeyboardButton("💬 Kuch Aur Poochna Hai", callback_data="contact_message")],
    ]
    if ADMIN_LINK:
        buttons.append([InlineKeyboardButton("📲 Admin se Seedha Baat Karo", url=ADMIN_LINK)])
    return InlineKeyboardMarkup(buttons)


# ══════════════════════════════════════
#           BASIC COMMANDS
# ══════════════════════════════════════

@app.on_message(filters.command("start"))
async def start_cmd(client, message: Message):
    await register_user(message)
    user_id = message.from_user.id if message.from_user else 0
    is_premium = await db.is_premium_user(user_id)
    premium_badge = "⭐ Premium" if is_premium else "👤 Free"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ Premium Lena Hai (Free!)", callback_data="contact_premium")],
        [InlineKeyboardButton("📊 My Status", callback_data="my_status")],
    ])
    await message.reply_text(
        f"**🎬 YouTube Auto Uploader Bot**\n\n"
        f"Video bhejo → YouTube pe upload → Link milega!\n\n"
        f"**Formats:** MP4, MKV, WebM, AVI, MOV\n"
        f"**Mode:** Unlisted 🔒\n"
        f"**Multi-Account:** Auto-rotate ✅\n\n"
        f"**Aapka Status:** {premium_badge}\n\n"
        f"⭐ **Premium bilkul FREE hai!**\n"
        f"Sirf apna channel email admin ko bhejo → Premium milega!\n\n"
        f"/contact — Premium lo ya kuch poochho\n"
        f"/mypremium — Apna status\n"
        f"/links — Recent uploads",
        reply_markup=kb
    )


@app.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    await register_user(message)
    await message.reply_text(
        "**📖 Commands**\n\n"
        "**📹 Upload**\n"
        "Bas video bhejo — auto upload hoga!\n\n"
        "**📊 Data Commands**\n"
        "/links — Last 10 uploads\n"
        "/search TITLE — Video dhundho\n"
        "/stats — Total stats\n"
        "/botstats — Detailed user stats\n\n"
        "**🎬 YouTube Account (Premium)**\n"
        "/addaccount NAME — Apna YT account add karo\n"
        "/accounts — Sare accounts dekho\n"
        "/mypremium — Apna premium status\n\n"
        "**⭐ Premium (Bilkul FREE!)**\n"
        "Sirf /contact pe tap karo → Email share karo → Premium milega!\n\n"
        "**📩 Support**\n"
        "/contact — Premium lo ya admin se baat karo",
        reply_markup=contact_keyboard()
    )


@app.on_message(filters.command("mypremium"))
async def mypremium_cmd(client, message: Message):
    await register_user(message)
    user_id = message.from_user.id if message.from_user else 0
    is_premium = await db.is_premium_user(user_id)

    if is_premium:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Premium Remove Karna Hai", callback_data="remove_premium_req")]
        ])
        await message.reply_text(
            "**⭐ Aap Premium User hain!**\n\n"
            "✅ `/addaccount` se apna YouTube channel add kar sakte ho\n"
            "✅ Unlimited video upload\n\n"
            "**YouTube Account Add karne ke steps:**\n"
            "1. `/addaccount APNA_NAAM` bhejo\n"
            "2. Auth link pe jao\n"
            "3. Google login karo\n"
            "4. Code copy karke `/code NAAM CODE` bhejo",
            reply_markup=kb
        )
    else:
        await message.reply_text(
            "**👤 Aap Free User hain**\n\n"
            "⭐ **Premium bilkul FREE hai!**\n"
            "Sirf apni Gmail ID share karo → Admin add kar dega!\n\n"
            "Neeche button tap karo 👇",
            reply_markup=contact_keyboard()
        )


# ══════════════════════════════════════
#       CONTACT / SECRET CHAT
# ══════════════════════════════════════

@app.on_message(filters.command("contact"))
async def contact_cmd(client, message: Message):
    await register_user(message)
    await message.reply_text(
        "**📩 Admin se contact karo**\n\n"
        "Neeche se option chunno 👇",
        reply_markup=contact_keyboard()
    )


async def _forward_to_admin(client, user, user_msg: str, msg_type: str):
    username = f"@{user.username}" if user.username else "N/A"
    type_label = "⭐ Premium Request" if msg_type == "premium" else "💬 Message"
    try:
        sent = await client.send_message(
            OWNER_ID,
            f"**📩 {type_label}**\n\n"
            f"👤 **User:** {user.first_name}\n"
            f"🔗 **Username:** {username}\n"
            f"🆔 **ID:** `{user.id}`\n\n"
            f"**💬 Message:**\n{user_msg}\n\n"
            f"💡 _Reply karo iss message pe — seedha user tak pahunch jayega!_\n"
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
        "_Bas email type karo aur send karo_ 👇",
    )
    await callback.answer()


@app.on_callback_query(filters.regex("^contact_message$"))
async def cb_contact_message(client, callback: CallbackQuery):
    user_id = callback.from_user.id
    contact_state[user_id] = "message"
    await callback.message.edit_text(
        "**💬 Admin ko Message**\n\n"
        "Apna message type karo aur send karo 👇\n\n"
        "_Admin jald reply karenge._"
    )
    await callback.answer()


@app.on_callback_query(filters.regex("^my_status$"))
async def cb_my_status(client, callback: CallbackQuery):
    user_id = callback.from_user.id
    is_premium = await db.is_premium_user(user_id)
    badge = "⭐ Premium" if is_premium else "👤 Free"
    await callback.answer(f"Aapka status: {badge}", show_alert=True)


@app.on_callback_query(filters.regex("^remove_premium_req$"))
async def cb_remove_premium_req(client, callback: CallbackQuery):
    user = callback.from_user
    username = f"@{user.username}" if user.username else "N/A"
    try:
        await client.send_message(
            OWNER_ID,
            f"**❌ Premium Remove Request**\n\n"
            f"👤 **User:** {user.first_name}\n"
            f"🔗 **Username:** {username}\n"
            f"🆔 **ID:** `{user.id}`\n\n"
            f"Is user ka premium remove karne ke liye:\n"
            f"`/removepremium {user.id}`"
        )
        await callback.answer("✅ Request bhej di gayi! Admin process karenge.", show_alert=True)
    except Exception:
        await callback.answer("❌ Request nahi gayi, dobara try karo.", show_alert=True)


@app.on_message(filters.text & ~filters.command([
    "start","help","mypremium","contact","addaccount","code","accounts",
    "links","search","stats","botstats","reply","broadcast","addpremium",
    "removepremium","premiumlist"
]) & ~filters.user(OWNER_ID))
async def handle_contact_reply(client, message: Message):
    await register_user(message)
    user_id = message.from_user.id if message.from_user else 0
    state = contact_state.pop(user_id, None)
    if not state:
        return

    user = message.from_user
    user_text = message.text.strip()

    if state == "premium":
        full_msg = f"📧 Email: {user_text}\n\nMujhe premium chahiye."
    else:
        full_msg = user_text

    success = await _forward_to_admin(client, user, full_msg, state)
    if success:
        if state == "premium":
            await message.reply_text(
                "**✅ Premium Request Admin ko pahunch gayi!**\n\n"
                "Admin aapki Gmail ID verify karke premium add kar denge.\n"
                "Aapko notification milega jab premium milega! 🎉\n\n"
                + (f"**Ya seedha baat karo:** {ADMIN_LINK}" if ADMIN_LINK else "")
            )
        else:
            await message.reply_text(
                "**✅ Message admin ko pahunch gaya!**\n\n"
                "Admin jald reply karenge.\n\n"
                + (f"**Ya seedha baat karo:** {ADMIN_LINK}" if ADMIN_LINK else "")
            )
    else:
        await message.reply_text(
            "❌ Message nahi bheja ja saka.\n"
            + (ADMIN_LINK if ADMIN_LINK else "Admin se seedha contact karo.")
        )


@app.on_message(filters.command("reply") & filters.user(OWNER_ID))
async def reply_user_cmd(client, message: Message):
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        await message.reply_text(
            "**Usage:** `/reply USER_ID Aapka jawab`\n\n"
            "**Example:** `/reply 123456789 Haan, aapko premium de diya gaya!`"
        )
        return

    try:
        target_id = int(parts[1].strip())
    except ValueError:
        await message.reply_text("❌ Valid User ID daalo.")
        return

    reply_text = parts[2].strip()

    try:
        await client.send_message(
            target_id,
            f"**📩 Admin ka jawab:**\n\n{reply_text}"
        )
        await message.reply_text(f"✅ Reply `{target_id}` ko bhej di gayi!")
    except Exception as e:
        await message.reply_text(f"❌ Reply nahi bhej saka: `{e}`")


@app.on_message(filters.user(OWNER_ID) & filters.reply & ~filters.command(["reply", "broadcast", "addpremium", "removepremium", "premiumlist", "addaccount", "accounts", "links", "search", "stats", "botstats", "start", "help", "contact", "code"]))
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
        await client.send_message(
            target_user_id,
            f"**📩 Admin ka jawab:**\n\n{reply_text}"
        )
        await message.reply_text(f"✅ Reply user `{target_user_id}` ko deliver ho gayi!")
    except Exception as e:
        await message.reply_text(f"❌ Deliver nahi hui: `{e}`")


# ══════════════════════════════════════
#       BROADCAST (OWNER ONLY)
# ══════════════════════════════════════

@app.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def broadcast_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        total = await db.get_total_users()
        await message.reply_text(
            f"**Usage:** `/broadcast Aapka message`\n\n"
            f"**Example:** `/broadcast Bot update ho gaya!`\n\n"
            f"📊 Total users: `{total}`"
        )
        return

    broadcast_text = parts[1].strip()
    user_ids = await db.get_all_user_ids()

    status_msg = await message.reply_text(
        f"📡 **Broadcast shuru ho raha hai...**\n\n"
        f"Total users: `{len(user_ids)}`"
    )

    success = 0
    failed = 0

    for uid in user_ids:
        try:
            await client.send_message(uid, f"📢 **Admin ka message:**\n\n{broadcast_text}")
            success += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    await status_msg.edit_text(
        f"**✅ Broadcast Complete!**\n\n"
        f"✅ Bheja: `{success}`\n"
        f"❌ Failed: `{failed}`\n"
        f"📊 Total: `{len(user_ids)}`"
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
            "**Example:** `/addpremium 123456789`"
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
    try:
        await client.send_message(
            target_id,
            "**🎉 Congratulations! Aapko Premium mil gaya!**\n\n"
            "⭐ Ab aap `/addaccount` se apna YouTube channel add kar sakte ho!\n\n"
            "**Steps:**\n"
            "1. `/addaccount APNA_NAAM` bhejo\n"
            "2. Auth link pe jao → Login karo\n"
            "3. Code copy karke `/code NAAM CODE` bhejo"
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
    await register_user(message)
    user_id = message.from_user.id if message.from_user else 0
    is_owner = (user_id == OWNER_ID)
    is_premium = await db.is_premium_user(user_id)

    if not is_owner and not is_premium:
        await message.reply_text(
            "**❌ Sirf Premium Users hi YouTube account add kar sakte hain!**\n\n"
            "**⭐ Premium lene ke liye:**\n"
            "• Admin aapka Telegram ID premium list mein add karega\n"
            "• Phir aap apna YouTube channel connect kar sakte ho\n\n"
            + contact_info_text()
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
            f"3️⃣ **Aapki Gmail** Google Cloud Console mein **test user** ke roop mein add honi chahiye — warna upload fail hoga!\n\n"
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
    await register_user(message)
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
            f"**Baar baar fail ho raha hai? Check karo:**\n"
            f"• Aapki Gmail Google Cloud Console mein **test user** ke roop mein add hai?\n"
            f"• YouTube channel bana hua hai?\n"
            f"• Phone number verify hai Google account mein?"
        )


@app.on_message(filters.command("accounts"))
async def accounts_cmd(client, message: Message):
    await register_user(message)
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
    await register_user(message)
    total = await db.get_total_count()
    total_size = await db.get_total_size()
    acc_count = youtube.get_account_count()
    total_users = await db.get_total_users()
    premium_users = await db.get_premium_users()
    await message.reply_text(
        f"**📊 Bot Stats**\n\n"
        f"🎬 Total Videos: `{total}`\n"
        f"💾 Total Size: `{total_size:.1f} MB`\n"
        f"👤 Active YT Accounts: `{acc_count}`\n"
        f"📈 Daily Capacity: `~{acc_count * 6}` videos\n"
        f"👥 Total Users: `{total_users}`\n"
        f"⭐ Premium Users: `{len(premium_users)}`"
    )


@app.on_message(filters.command("botstats"))
async def botstats_cmd(client, message: Message):
    await register_user(message)
    now = datetime.utcnow()
    today     = now.replace(hour=0, minute=0, second=0, microsecond=0)
    this_week = now - timedelta(days=7)
    this_month = now - timedelta(days=30)

    total_users   = await db.get_total_users()
    daily_new     = await db.get_users_since(today)
    weekly_new    = await db.get_users_since(this_week)
    monthly_new   = await db.get_users_since(this_month)

    daily_active  = await db.get_active_users_since(today)
    weekly_active = await db.get_active_users_since(this_week)
    monthly_active= await db.get_active_users_since(this_month)

    daily_vids    = await db.get_videos_since(today)
    weekly_vids   = await db.get_videos_since(this_week)
    monthly_vids  = await db.get_videos_since(this_month)
    total_vids    = await db.get_total_count()

    premium_count = len(await db.get_premium_users())

    await message.reply_text(
        f"**📈 Bot Detailed Stats**\n\n"
        f"**👥 New Users Joined:**\n"
        f"• Aaj:        `{daily_new}`\n"
        f"• Is Hafte:   `{weekly_new}`\n"
        f"• Is Mahine:  `{monthly_new}`\n"
        f"• Overall:    `{total_users}`\n\n"
        f"**🟢 Active Users (last seen):**\n"
        f"• Aaj:        `{daily_active}`\n"
        f"• Is Hafte:   `{weekly_active}`\n"
        f"• Is Mahine:  `{monthly_active}`\n\n"
        f"**🎬 Videos Uploaded:**\n"
        f"• Aaj:        `{daily_vids}`\n"
        f"• Is Hafte:   `{weekly_vids}`\n"
        f"• Is Mahine:  `{monthly_vids}`\n"
        f"• Overall:    `{total_vids}`\n\n"
        f"**⭐ Premium Users: `{premium_count}`**"
    )


# ══════════════════════════════════════
#        UPLOAD QUEUE WORKER
# ══════════════════════════════════════

async def process_upload(client, message: Message, status_msg, file_unique_id,
                         title, caption, file_size, size_mb):
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
        await status_msg.edit_text(
            f"**📥 Download shuru...**\n\n"
            f"📌 `{title}`\n"
            f"📦 `{size_mb:.1f} MB`\n"
            f"👤 Accounts: `{youtube.get_account_count()}` (auto-rotate ON)"
        )

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
            user_id = message.from_user.id if message.from_user else 0
            username = message.from_user.username if message.from_user else "unknown"
            await db.save_pending_upload(
                chat_id=message.chat.id,
                message_id=message.id,
                title=title,
                caption=caption,
                file_size=file_size,
                size_mb=size_mb,
                user_id=user_id,
                username=username
            )
            await status_msg.edit_text(
                "⚠️ **YouTube Quota Khatam Ho Gaya!**\n\n"
                f"📌 `{title}`\n"
                f"📦 `{size_mb:.1f} MB`\n\n"
                "✅ **Teri video save kar li gayi hai!**\n"
                "YouTube quota kal subah ~8:30 AM UTC pe reset hoga.\n"
                "**Dobara bhejne ki zaroorat nahi — kal automatically upload ho jaayega!** 🔄\n\n"
                "📋 Status check karna ho to `/pending` use karo."
            )

    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.error(f"Upload error: {e}", exc_info=True)
        try:
            await status_msg.edit_text(f"❌ Error: `{str(e)[:300]}`")
        except Exception:
            pass
    finally:
        active_uploads.pop(file_unique_id, None)
        queue_positions.pop(file_unique_id, None)
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except Exception:
                pass
        shutil.rmtree(tmp_dir, ignore_errors=True)


async def upload_worker(worker_id: int):
    logger.info(f"Upload Worker #{worker_id} ready")
    while True:
        try:
            task = await upload_queue.get()
            client, message, status_msg, file_unique_id, title, caption, file_size, size_mb = task
            try:
                await process_upload(client, message, status_msg, file_unique_id,
                                     title, caption, file_size, size_mb)
            except Exception as e:
                logger.error(f"Worker #{worker_id} error: {e}", exc_info=True)
            finally:
                upload_queue.task_done()
        except Exception as e:
            logger.error(f"Worker #{worker_id} fatal: {e}", exc_info=True)
            await asyncio.sleep(1)


# ══════════════════════════════════════
#           VIDEO HANDLER
# ══════════════════════════════════════

@app.on_message(filters.video | filters.document)
async def handle_video(client, message: Message):
    await register_user(message)
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

    file_unique_id = getattr(file, 'file_unique_id', None) or str(message.id)

    if file_unique_id in active_uploads:
        await message.reply_text(
            "⚠️ **Yeh video already queue mein hai!**\n\n"
            "Pehle wali upload complete hone ka wait karo."
        )
        return

    active_uploads[file_unique_id] = True

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

    queue_size = upload_queue.qsize()
    if queue_size == 0:
        status_msg = await message.reply_text(
            f"**⏳ Queue mein add ho gaya...**\n\n"
            f"📌 `{title}`\n"
            f"📦 `{size_mb:.1f} MB`\n"
            f"🔢 Position: `#1` — Abhi shuru hoga!"
        )
    else:
        status_msg = await message.reply_text(
            f"**📋 Queue mein add ho gaya**\n\n"
            f"📌 `{title}`\n"
            f"📦 `{size_mb:.1f} MB`\n"
            f"🔢 Position: `#{queue_size + 1}` — Wait karo..."
        )

    queue_positions[file_unique_id] = status_msg
    await upload_queue.put((client, message, status_msg, file_unique_id,
                            title, caption, file_size, size_mb))


# ══════════════════════════════════════
#        PENDING UPLOADS RETRY
# ══════════════════════════════════════

async def retry_pending_uploads(client):
    pending = await db.get_pending_uploads()
    if not pending:
        logger.info("Pending retry: koi pending upload nahi hai")
        return

    logger.info(f"Pending retry: {len(pending)} videos retry ho rahe hain...")

    for doc in pending:
        doc_id    = doc["_id"]
        chat_id   = doc["chat_id"]
        message_id = doc["message_id"]
        title     = doc["title"]
        caption   = doc["caption"]
        file_size = doc["file_size"]
        size_mb   = doc["size_mb"]
        user_id   = doc["user_id"]

        try:
            orig_msg = await client.get_messages(chat_id, message_id)
            if not orig_msg or not (orig_msg.video or orig_msg.document):
                logger.warning(f"Pending: message {message_id} Telegram pe nahi mila, skip")
                await db.delete_pending_upload(doc_id)
                try:
                    await client.send_message(
                        user_id,
                        f"⚠️ Pending video **'{title}'** ka original message Telegram pe expire ho gaya.\n"
                        "Dobara bhejni padegi."
                    )
                except Exception:
                    pass
                continue

            notify_msg = await client.send_message(
                user_id,
                f"🔄 **Auto-Retry Shuru!**\n\n"
                f"📌 `{title}`\n"
                f"📦 `{size_mb:.1f} MB`\n\n"
                "YouTube quota reset ho gaya. Upload ho raha hai..."
            )

            await db.increment_pending_retry(doc_id)
            await upload_queue.put((
                client, orig_msg, notify_msg,
                getattr(orig_msg.video or orig_msg.document, 'file_unique_id', str(message_id)),
                title, caption, file_size, size_mb
            ))

            active_uploads[
                getattr(orig_msg.video or orig_msg.document, 'file_unique_id', str(message_id))
            ] = True

            await db.delete_pending_upload(doc_id)

        except Exception as e:
            logger.error(f"Pending retry error for msg {message_id}: {e}")


async def daily_retry_scheduler(client):
    while True:
        now_utc = datetime.utcnow()
        next_retry = now_utc.replace(hour=8, minute=30, second=0, microsecond=0)
        if now_utc >= next_retry:
            next_retry = next_retry + timedelta(days=1)
        wait_secs = (next_retry - now_utc).total_seconds()
        logger.info(f"Daily retry scheduler: {wait_secs/3600:.1f} ghante baad chalega ({next_retry} UTC)")
        await asyncio.sleep(wait_secs)
        logger.info("Daily retry: YouTube quota reset ho gaya, pending uploads retry ho rahe hain...")
        try:
            await retry_pending_uploads(client)
        except Exception as e:
            logger.error(f"Daily retry error: {e}", exc_info=True)


@app.on_message(filters.command("pending") & filters.user(OWNER_ID))
async def pending_command(client, message: Message):
    pending = await db.get_pending_uploads()
    if not pending:
        await message.reply_text("✅ Koi pending upload nahi hai!")
        return

    lines = [f"**📋 Pending Uploads: `{len(pending)}`**\n"]
    for i, doc in enumerate(pending[:20], 1):
        saved = doc["saved_at"].strftime("%d %b %H:%M UTC")
        lines.append(
            f"`{i}.` **{doc['title'][:35]}**\n"
            f"    👤 `{doc['user_id']}` | 📦 `{doc['size_mb']:.1f} MB` | 🔁 `{doc['retry_count']}x`\n"
            f"    ⏰ `{saved}`"
        )
    if len(pending) > 20:
        lines.append(f"\n_...aur `{len(pending) - 20}` videos_")
    lines.append("\n\n💡 Abhi retry karna hai? `/retrypending` use karo.")
    await message.reply_text("\n".join(lines))


@app.on_message(filters.command("retrypending") & filters.user(OWNER_ID))
async def force_retry_command(client, message: Message):
    count = await db.get_pending_count()
    if count == 0:
        await message.reply_text("✅ Koi pending upload nahi hai!")
        return
    await message.reply_text(
        f"🔄 **{count} pending videos ka retry shuru ho raha hai...**\n"
        "Har video queue mein add hogi. Users ko notify kiya jaayega."
    )
    await retry_pending_uploads(client)


if __name__ == "__main__":
    set_bot_commands_via_api()
    Thread(target=start_health_server, daemon=True).start()
    logger.info("Bot starting...")

    async def main():
        async with app:
            for i in range(UPLOAD_WORKERS):
                asyncio.create_task(upload_worker(i + 1))
            asyncio.create_task(daily_retry_scheduler(app))
            logger.info(f"✅ {UPLOAD_WORKERS} upload workers + daily retry scheduler started")
            await idle()

    app.run(main())
