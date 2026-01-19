import os
import sqlite3

# -------------------------------------------------
# SQLite path:
# - Render/Linux: /tmp é sempre escrevível
# - Local/Windows: usa pasta db/ dentro do projeto
# Permite override com env var SERVICE2_DB_PATH
# -------------------------------------------------
def _default_db_path() -> str:
    env_path = os.getenv("SERVICE2_DB_PATH")
    if env_path:
        return env_path

    # Em Linux/cloud, /tmp é ok
    if os.name != "nt":
        return "/tmp/service2.db"

    # Em Windows/local, cria db/service2.db
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # .../app
    db_dir = os.path.join(base_dir, "db")
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, "service2.db")


DB_PATH = _default_db_path()


def get_connection():
    # garante pasta existe caso DB_PATH seja relativo/Windows
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(
        DB_PATH,
        timeout=30,
        check_same_thread=False,
        isolation_level=None,  # autocommit; evita transações “presas”
    )
    conn.row_factory = sqlite3.Row

    # pragmas por ligação (ajudam muito em concorrência)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")  # espera 5s antes de “database is locked”

    return conn


def init_db():
    # init idempotente
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE,
                status TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                processed_at TEXT,
                worker_id TEXT
            )
            """
        )
        # índice ajuda o claim_next_file ficar rápido
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_file_queue_status_detected
            ON file_queue(status, detected_at)
            """
        )
