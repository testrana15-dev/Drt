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
        self.pending_col = self.db["pending_uploads"]
        # ✅ FIX: contact_reply_map ab MongoDB mein save hoga — restart pe nahi jayega
        self.reply_map_col = self.db["admin_reply_map"]
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

    async def get_users_since(self, since_dt):
        return await self.users_col.count_documents({"joined_at": {"$gte": since_dt}})

    async def get_active_users_since(self, since_dt):
        return await self.users_col.count_documents({"last_seen": {"$gte": since_dt}})

    async def get_videos_since(self, since_dt):
        return await self.col.count_documents({"uploaded_at": {"$gte": since_dt}})

    # ── Pending Uploads (quota exceeded → retry next day) ──
    async def save_pending_upload(self, chat_id: int, message_id: int,
                                   title: str, caption: str,
                                   file_size: int, size_mb: float,
                                   user_id: int, username: str):
        doc = {
            "chat_id": chat_id,
            "message_id": message_id,
            "title": title,
            "caption": caption,
            "file_size": file_size,
            "size_mb": size_mb,
            "user_id": user_id,
            "username": username,
            "saved_at": datetime.utcnow(),
            "retry_count": 0
        }
        await self.pending_col.insert_one(doc)

    async def get_pending_uploads(self):
        cursor = self.pending_col.find().sort("saved_at", 1)
        return await cursor.to_list(length=500)

    async def delete_pending_upload(self, doc_id):
        await self.pending_col.delete_one({"_id": doc_id})

    async def increment_pending_retry(self, doc_id):
        await self.pending_col.update_one(
            {"_id": doc_id},
            {"$inc": {"retry_count": 1}, "$set": {"last_retry": datetime.utcnow()}}
        )

    async def get_pending_count(self):
        return await self.pending_col.count_documents({})

    # ── ✅ FIX: Admin Reply Map — MongoDB mein persist hoga ──
    async def save_reply_map(self, admin_msg_id: int, user_id: int):
        """Admin ka forwarded message ID → original user ID mapping save karo"""
        await self.reply_map_col.update_one(
            {"admin_msg_id": admin_msg_id},
            {"$set": {
                "admin_msg_id": admin_msg_id,
                "user_id": user_id,
                "saved_at": datetime.utcnow()
            }},
            upsert=True
        )

    async def get_reply_map_user(self, admin_msg_id: int):
        """Admin message ID se user ID nikalo"""
        doc = await self.reply_map_col.find_one({"admin_msg_id": admin_msg_id})
        return doc["user_id"] if doc else None

    async def cleanup_old_reply_maps(self, days: int = 7):
        """Purane reply maps clean karo (7 din se purane)"""
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=days)
        await self.reply_map_col.delete_many({"saved_at": {"$lt": cutoff}})
