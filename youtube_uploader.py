import os
import json
import logging
import asyncio
from datetime import datetime

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import pymongo  # Sync client — kyunki yeh thread executor mein chalta hai

logger = logging.getLogger(__name__)

CLIENT_ID = "79361501505-f828kv7g49ud8m6telgt17ne3l4qpobl.apps.googleusercontent.com"
CLIENT_SECRET = "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"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
CREDS_FILE = "client_secrets.json"


class YouTubeUploader:
    def __init__(self, mongo_uri: str):
        self.service = None
        self._flow = None
        self._mongo_uri = mongo_uri
        self._mongo_col = None  # Lazy init
        self._create_client_secrets()
        self._load_credentials()

    # ─── Sync MongoDB (pymongo) for token — runs in thread executor ────────────

    def _get_mongo_col(self):
        if self._mongo_col is None:
            client = pymongo.MongoClient(self._mongo_uri)
            db = client["yt_uploader_bot"]
            self._mongo_col = db["auth_tokens"]
        return self._mongo_col

    def _save_token_to_db(self, creds: Credentials):
        """Token dict MongoDB mein save karo — Render restart pe bhi safe rahega."""
        token_dict = {
            "token": creds.token,
            "refresh_token": creds.refresh_token,
            "token_uri": creds.token_uri,
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "scopes": list(creds.scopes) if creds.scopes else [],
            "expiry": creds.expiry.isoformat() if creds.expiry else None,
        }
        col = self._get_mongo_col()
        col.update_one(
            {"_id": "youtube_token"},
            {"$set": {"token": token_dict, "updated_at": datetime.utcnow()}},
            upsert=True
        )
        logger.info("YouTube token MongoDB mein save ho gaya!")

    def _load_token_from_db(self) -> Credentials | None:
        """MongoDB se token lo aur Credentials object banao."""
        try:
            col = self._get_mongo_col()
            doc = col.find_one({"_id": "youtube_token"})
            if not doc:
                return None
            t = doc["token"]
            expiry = None
            if t.get("expiry"):
                try:
                    expiry = datetime.fromisoformat(t["expiry"])
                except Exception:
                    pass
            creds = Credentials(
                token=t.get("token"),
                refresh_token=t.get("refresh_token"),
                token_uri=t.get("token_uri", "https://oauth2.googleapis.com/token"),
                client_id=t.get("client_id"),
                client_secret=t.get("client_secret"),
                scopes=t.get("scopes", SCOPES),
            )
            creds.expiry = expiry
            return creds
        except Exception as e:
            logger.error(f"Token load error: {e}")
            return None

    # ─── Setup ─────────────────────────────────────────────────────────────────

    def _create_client_secrets(self):
        secrets = {
            "installed": {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token"
            }
        }
        with open(CREDS_FILE, "w") as f:
            json.dump(secrets, f)

    def _load_credentials(self):
        creds = self._load_token_from_db()
        if not creds:
            logger.info("MongoDB mein koi token nahi. /auth se authorize karo.")
            return

        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                self._save_token_to_db(creds)
                logger.info("Token refresh ho gaya aur save ho gaya!")
            except Exception as e:
                logger.error(f"Token refresh error: {e}")
                return

        if creds.valid:
            self._build_service(creds)

    def _build_service(self, creds: Credentials):
        self.service = build("youtube", "v3", credentials=creds)
        logger.info("YouTube service ready!")

    # ─── Auth ──────────────────────────────────────────────────────────────────

    def get_auth_url(self) -> str | None:
        if self.service:
            return None  # Already authorized
        try:
            self._flow = InstalledAppFlow.from_client_secrets_file(
                CREDS_FILE, SCOPES,
                redirect_uri="urn:ietf:wg:oauth:2.0:oob"
            )
            auth_url, _ = self._flow.authorization_url(
                access_type="offline",
                include_granted_scopes="true",
                prompt="consent"
            )
            return auth_url
        except Exception as e:
            logger.error(f"Auth URL error: {e}")
            return None

    def authenticate_with_code(self, code: str) -> bool:
        try:
            if not self._flow:
                self._flow = InstalledAppFlow.from_client_secrets_file(
                    CREDS_FILE, SCOPES,
                    redirect_uri="urn:ietf:wg:oauth:2.0:oob"
                )
            self._flow.fetch_token(code=code)
            creds = self._flow.credentials
            self._save_token_to_db(creds)   # ← MongoDB mein save
            self._build_service(creds)
            return True
        except Exception as e:
            logger.error(f"Auth code error: {e}")
            return False

    def is_authorized(self) -> bool:
        return self.service is not None

    # ─── Upload ────────────────────────────────────────────────────────────────

    def upload_video(self, file_path: str, title: str, description: str = "",
                     privacy: str = "unlisted", progress_queue=None, loop=None):
        if not self.service:
            logger.error("YouTube authorized nahi hai!")
            return None, None

        try:
            file_size = os.path.getsize(file_path)
            body = {
                "snippet": {
                    "title": title[:100],
                    "description": description,
                    "tags": [],
                    "categoryId": "22"
                },
                "status": {
                    "privacyStatus": privacy,
                    "selfDeclaredMadeForKids": False
                }
            }
            chunk_size = 100 * 1024 * 1024  # 100MB chunks
            media = MediaFileUpload(file_path, chunksize=chunk_size, resumable=True)
            request = self.service.videos().insert(
                part=",".join(body.keys()),
                body=body,
                media_body=media
            )

            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    uploaded_bytes = int(status.resumable_progress)
                    percent = status.progress() * 100
                    logger.info(f"Upload: {percent:.1f}%")
                    if progress_queue and loop:
                        try:
                            asyncio.run_coroutine_threadsafe(
                                progress_queue.put((percent, uploaded_bytes, file_size)),
                                loop
                            )
                        except Exception:
                            pass

            if progress_queue and loop:
                try:
                    asyncio.run_coroutine_threadsafe(progress_queue.put(None), loop)
                except Exception:
                    pass

            video_id = response.get("id")
            if video_id:
                yt_url = f"https://youtu.be/{video_id}"
                logger.info(f"Upload complete: {yt_url}")
                return yt_url, video_id
            return None, None

        except HttpError as e:
            logger.error(f"YouTube HTTP error: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Upload error: {e}", exc_info=True)
            return None, None