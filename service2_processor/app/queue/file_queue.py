from datetime import datetime, timezone
from .db import get_connection


# -------------------------
# Helpers
# -------------------------
def _now_utc():
    return datetime.now(timezone.utc)


# -------------------------
# Enfileirar novo ficheiro
# -------------------------
def enqueue_file(file_path: str):
    """
    Insere ficheiro novo com estado NEW.
    PostgreSQL: usa ON CONFLICT DO NOTHING.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO file_queue (file_path, status, detected_at)
                VALUES (%s, 'NEW', %s)
                ON CONFLICT (file_path) DO NOTHING
                """,
                (file_path, _now_utc()),
            )
        conn.commit()


# -------------------------
# Worker reclama ficheiro
# -------------------------
def claim_next_file(worker_id: str):
    """
    Reclama atomicamente UM ficheiro NEW.
    PostgreSQL: SELECT ... FOR UPDATE SKIP LOCKED evita que 2 workers peguem no mesmo.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1) escolhe 1 registo NEW e tranca só esse registo
            cur.execute(
                """
                SELECT id, file_path
                FROM file_queue
                WHERE status = 'NEW'
                ORDER BY detected_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None

            # row é dict se usares RealDictCursor, então:
            file_id = row["id"]
            file_path = row["file_path"]

            # 2) marca como PROCESSING
            cur.execute(
                """
                UPDATE file_queue
                SET status = 'PROCESSING',
                    worker_id = %s
                WHERE id = %s
                """,
                (worker_id, file_id),
            )

        conn.commit()
        return file_path


# -------------------------
# Marcar como DONE
# -------------------------
def mark_done(file_path: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE file_queue
                SET status = 'DONE',
                    processed_at = %s
                WHERE file_path = %s
                """,
                (_now_utc(), file_path),
            )
        conn.commit()


# -------------------------
# Marcar como ERROR
# -------------------------
def mark_error(file_path: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE file_queue
                SET status = 'ERROR',
                    processed_at = %s
                WHERE file_path = %s
                """,
                (_now_utc(), file_path),
            )
        conn.commit()


# -------------------------
# Verificar se ficheiro já existe na fila
# -------------------------
def file_exists(file_path: str) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM file_queue
                WHERE file_path = %s
                LIMIT 1
                """,
                (file_path,),
            )
            return cur.fetchone() is not None
