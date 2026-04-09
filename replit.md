# YouTube Auto Uploader Telegram Bot

## Project Overview
Telegram bot jo videos receive karta hai aur automatically YouTube pe upload karta hai (Unlisted mode). Multi-account support ke saath auto-rotation hoti hai.

## Tech Stack
- **Language:** Python 3.11
- **Telegram:** Pyrogram v2.0.106
- **YouTube API:** google-api-python-client
- **Database:** MongoDB (motor async driver)
- **Entry Point:** `bot.py`

## Architecture
- `bot.py` — Main bot logic, commands, video handler
- `youtube_uploader.py` — YouTube OAuth2 + multi-account upload
- `database.py` — MongoDB operations

## Required Environment Variables

| Variable | Description |
|----------|-------------|
| `BOT_TOKEN` | Telegram Bot Token (@BotFather se) |
| `API_ID` | Telegram API ID (my.telegram.org) |
| `API_HASH` | Telegram API Hash (my.telegram.org) |
| `MONGO_URI` | MongoDB connection string |
| `OWNER_ID` | Telegram Owner User ID |
| `YOUTUBE_CLIENT_ID` | Google OAuth2 Client ID |
| `YOUTUBE_CLIENT_SECRET` | Google OAuth2 Client Secret |
| `LOG_CHANNEL` | (Optional) Telegram channel ID for logs |
| `PORT` | (Auto-set by Railway) Health server port |

## Railway Deployment
- `railway.toml` — Railway build & deploy config
- `nixpacks.toml` — Python 3.11 environment setup
- `.railwayignore` — Files excluded from deploy

## Local Development
```bash
pip install -r requirements.txt
python bot.py
```
