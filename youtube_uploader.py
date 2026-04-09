import os
import pickle
import logging
import asyncio
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
CREDS_FILE = "client_secrets.json"
ACCOUNTS_DIR = "yt_accounts"


class YouTubeAccount:
    def __init__(self, account_id: str):
        self.account_id = account_id
        self.token_file = os.path.join(ACCOUNTS_DIR, f"token_{account_id}.pkl")
        self.service = None
        self._flow = None
        self._load_credentials()

    def _load_credentials(self):
        if not os.path.exists(self.token_file):
            return
        try:
            with open(self.token_file, "rb") as f:
                creds = pickle.load(f)
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                self._save_token(creds)
            if creds and creds.valid:
                self.service = build("youtube", "v3", credentials=creds)
                logger.info(f"Account {self.account_id} loaded ✅")
        except Exception as e:
            logger.error(f"Account {self.account_id} load error: {e}")

    def _save_token(self, creds):
        os.makedirs(ACCOUNTS_DIR, exist_ok=True)
        with open(self.token_file, "wb") as f:
            pickle.dump(creds, f)

    def get_auth_url(self):
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
            self._save_token(creds)
            self.service = build("youtube", "v3", credentials=creds)
            return True
        except Exception as e:
            logger.error(f"Auth code error: {e}")
            return False

    def is_ready(self):
        return self.service is not None

    def upload_video(self, file_path: str, title: str, description: str = "",
                     privacy: str = "unlisted", progress_queue=None, loop=None):
        if not self.service:
            return None, None, "not_authenticated"

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
            media = MediaFileUpload(
                file_path,
                chunksize=100 * 1024 * 1024,
                resumable=True
            )
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
                return f"https://youtu.be/{video_id}", video_id, "success"
            return None, None, "no_id"

        except HttpError as e:
            error_str = str(e)
            if "uploadLimitExceeded" in error_str:
                logger.warning(f"Account {self.account_id} limit exceeded!")
                return None, None, "limit_exceeded"
            logger.error(f"HTTP error: {e}")
            return None, None, "http_error"
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return None, None, str(e)


class YouTubeUploader:
    """Multi-account YouTube uploader with auto-rotate"""

    def __init__(self):
        os.makedirs(ACCOUNTS_DIR, exist_ok=True)
        self._create_client_secrets()
        self.accounts = {}
        self._pending_auth = {}
        self._load_all_accounts()

    def _create_client_secrets(self):
        CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID", "")
        CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET", "")
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

    def _load_all_accounts(self):
        if not os.path.exists(ACCOUNTS_DIR):
            return
        for fname in os.listdir(ACCOUNTS_DIR):
            if fname.startswith("token_") and fname.endswith(".pkl"):
                acc_id = fname[6:-4]
                acc = YouTubeAccount(acc_id)
                if acc.is_ready():
                    self.accounts[acc_id] = acc
                    logger.info(f"Loaded account: {acc_id}")

    def get_account_count(self):
        return len(self.accounts)

    def get_accounts_status(self):
        if not self.accounts:
            return "❌ Koi account authorized nahi hai"
        lines = []
        for i, acc_id in enumerate(self.accounts, 1):
            lines.append(f"{i}. ✅ Account `{acc_id}`")
        return "\n".join(lines)

    def start_auth(self, account_id: str):
        acc = YouTubeAccount(account_id)
        url = acc.get_auth_url()
        if url:
            self._pending_auth[account_id] = acc
        return url

    def finish_auth(self, account_id: str, code: str) -> bool:
        acc = self._pending_auth.get(account_id)
        if not acc:
            acc = YouTubeAccount(account_id)
            acc.get_auth_url()

        success = acc.authenticate_with_code(code)
        if success:
            self.accounts[account_id] = acc
            if account_id in self._pending_auth:
                del self._pending_auth[account_id]
        return success

    def remove_account(self, account_id: str) -> bool:
        if account_id in self.accounts:
            del self.accounts[account_id]
            token_file = os.path.join(ACCOUNTS_DIR, f"token_{account_id}.pkl")
            if os.path.exists(token_file):
                os.remove(token_file)
            return True
        return False

    def upload_video(self, file_path: str, title: str, description: str = "",
                     privacy: str = "unlisted", progress_queue=None, loop=None):
        if not self.accounts:
            return None, None, "no_accounts"

        account_ids = list(self.accounts.keys())

        for acc_id in account_ids:
            acc = self.accounts[acc_id]
            logger.info(f"Trying account: {acc_id}")

            yt_link, yt_id, status = acc.upload_video(
                file_path=file_path,
                title=title,
                description=description,
                privacy=privacy,
                progress_queue=progress_queue,
                loop=loop
            )

            if status == "success":
                logger.info(f"Upload success via account: {acc_id}")
                return yt_link, yt_id, "success"

            elif status == "limit_exceeded":
                logger.warning(f"Account {acc_id} limit exceeded, trying next...")
                continue

            else:
                logger.error(f"Account {acc_id} error: {status}")
                continue

        logger.error("All accounts exhausted!")
        return None, None, "all_failed"
