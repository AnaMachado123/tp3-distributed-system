import os
import psycopg2
from psycopg2.extras import RealDictCursor

# -------------------------------------------------
# PostgreSQL (cloud/local)
# Usa variável de ambiente:
#   SERVICE2_DATABASE_URL
# -------------------------------------------------

DATABASE_URL = os.getenv("SERVICE2_DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "SERVICE2_DATABASE_URL não definida. "
        "Configure a ligação PostgreSQL (Supabase/Render)."
    )


def get_connection():
    """
    Cria e devolve uma ligação PostgreSQL.
    Autocommit controlado manualmente.
    """
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor
    )


def init_db():
    """
    Inicialização idempotente da base de dados.
    Pode ser chamada em cada arranque do serviço.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS file_queue (
                    id SERIAL PRIMARY KEY,
                    file_path TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL,
                    detected_at TIMESTAMP NOT NULL,
                    processed_at TIMESTAMP,
                    worker_id TEXT
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_queue_status_detected
                ON file_queue(status, detected_at)
            """)

        conn.commit()
