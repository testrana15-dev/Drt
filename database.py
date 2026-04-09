import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, uri: str):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client["yt_uploader_bot"]
        self.col = self.db["videos"]
        self.premium_col = self.db["premium_users"]
        self.users_col = self.db["users"]
        logger.info("MongoDB connected!")

    # ── Videos ──────────────────────────────────────────

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

    # ── Premium Users ────────────────────────────────────

    async def add_premium_user(self, user_id: int, username: str = ""):
        await self.premium_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "username": username, "added_at": datetime.utcnow()}},
            upsert=True
        )

    async def remove_premium_user(self, user_id: int):
        result = await self.premium_col.delete_one({"user_id": user_id})
        return result.deleted_count > 0

    async def is_premium_user(self, user_id: int) -> bool:
        doc = await self.premium_col.find_one({"user_id": user_id})
        return doc is not None

    async def get_premium_users(self):
        cursor = self.premium_col.find().sort("added_at", -1)
        return await cursor.to_list(length=100)

    # ── All Users (for broadcast) ────────────────────────

    async def save_user(self, user_id: int, username: str = "", first_name: str = ""):
        await self.users_col.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_seen": datetime.utcnow()
            }, "$setOnInsert": {"joined_at": datetime.utcnow()}},
            upsert=True
        )

    async def get_all_user_ids(self):
        cursor = self.users_col.find({}, {"user_id": 1})
        docs = await cursor.to_list(length=100000)
        return [d["user_id"] for d in docs]

    async def get_total_users(self):
        return await self.users_col.count_documents({})
