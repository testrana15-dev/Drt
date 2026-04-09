import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, uri: str):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client["yt_uploader_bot"]
        self.col = self.db["videos"]
        logger.info("MongoDB connected!")

    async def save_video(self, title, caption, yt_link, yt_id, size_mb, user_id, username):
        doc = {
            "title": title,
            "caption": caption,
            "yt_link": yt_link,
            "yt_id": yt_id,
            "size_mb": size_mb,
            "user_id": user_id,
            "username": username,
            "uploaded_at": datetime.utcnow()
        }
        await self.col.insert_one(doc)

    async def get_recent_videos(self, limit=10):
        cursor = self.col.find().sort("uploaded_at", -1).limit(limit)
        return await cursor.to_list(length=limit)

    async def search_videos(self, query):
        cursor = self.col.find(
            {"title": {"$regex": query, "$options": "i"}}
        ).sort("uploaded_at", -1).limit(10)
        return await cursor.to_list(length=10)

    async def get_total_count(self):
        return await self.col.count_documents({})

    async def get_total_size(self):
        pipeline = [{"$group": {"_id": None, "total": {"$sum": "$size_mb"}}}]
        result = await self.col.aggregate(pipeline).to_list(1)
        return result[0]["total"] if result else 0.0
