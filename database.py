import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, uri: str):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client["yt_uploader_bot"]
        self.videos = self.db["videos"]
        self.tokens = self.db["auth_tokens"]
        logger.info("MongoDB connected!")

    # ─── YouTube Token (Permanent Storage) ────────────────────────────────────

    async def save_yt_token(self, token_dict: dict):
        """YouTube OAuth token ko MongoDB mein save karo — hamesha ke liye."""
        await self.tokens.update_one(
            {"_id": "youtube_token"},
            {"$set": {"token": token_dict, "updated_at": datetime.utcnow()}},
            upsert=True
        )
        logger.info("YouTube token MongoDB mein save ho gaya!")

    async def get_yt_token(self) -> dict | None:
        """MongoDB se YouTube token lo."""
        doc = await self.tokens.find_one({"_id": "youtube_token"})
        return doc["token"] if doc else None

    async def delete_yt_token(self):
        """Token delete karo (re-auth ke liye)."""
        await self.tokens.delete_one({"_id": "youtube_token"})

    # ─── Video Records ─────────────────────────────────────────────────────────

    async def is_duplicate(self, caption: str) -> dict | None:
        """Same caption wali video pehle upload hui hai? Agar haan to record return karo."""
        import re
        escaped = re.escape(caption.strip())
        doc = await self.videos.find_one(
            {"caption": {"$regex": f"^{escaped}$", "$options": "i"}}
        )
        return doc

    async def save_video(self, title: str, caption: str, yt_link: str,
                         yt_id: str, size_mb: float, user_id: int, username: str):
        doc = {
            "title": title,
            "caption": caption.strip(),
            "yt_link": yt_link,
            "yt_id": yt_id,
            "size_mb": round(size_mb, 2),
            "user_id": user_id,
            "username": username,
            "uploaded_at": datetime.utcnow()
        }
        await self.videos.insert_one(doc)
        logger.info(f"Video saved to DB: {title}")

    async def get_recent_videos(self, limit: int = 10):
        cursor = self.videos.find().sort("uploaded_at", -1).limit(limit)
        return await cursor.to_list(length=limit)

    async def search_videos(self, query: str):
        cursor = self.videos.find(
            {"title": {"$regex": query, "$options": "i"}}
        ).sort("uploaded_at", -1).limit(10)
        return await cursor.to_list(length=10)

    async def get_total_count(self):
        return await self.videos.count_documents({})

    async def get_total_size(self):
        pipeline = [{"$group": {"_id": None, "total": {"$sum": "$size_mb"}}}]
        result = await self.videos.aggregate(pipeline).to_list(1)
        return result[0]["total"] if result else 0.0