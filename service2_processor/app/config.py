import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    BUCKET_POLL_INTERVAL = int(os.getenv("BUCKET_POLL_INTERVAL", 30))
    NUM_WORKERS = int(os.getenv("NUM_WORKERS", 2))

    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")
    SUPABASE_PROCESSED_BUCKET = os.getenv("SUPABASE_PROCESSED_BUCKET")

    XML_SERVICE_URL = os.getenv("XML_SERVICE_URL")
    XML_SERVICE_INGEST_PATH = os.getenv("XML_SERVICE_INGEST_PATH", "/ingest")
    WEBHOOK_URL = os.getenv("WEBHOOK_URL")
    MAPPER_VERSION = os.getenv("MAPPER_VERSION", "custo_vida_v1")

    HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", 20))
    HTTP_RETRY_COUNT = int(os.getenv("HTTP_RETRY_COUNT", 3))

settings = Settings()
