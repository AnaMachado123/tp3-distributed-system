from datetime import datetime
import sqlite3
from .db import get_connection


# -------------------------
# Enfileirar novo ficheiro
# -------------------------
def enqueue_file(file_path: str):
    """
    Insere ficheiro novo com estado NEW
    """
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO file_queue (
                file_path, status, detected_at
            )
            VALUES (?, 'NEW', ?)
            """,
            (file_path, datetime.now().isoformat())
        )


# -------------------------
# Worker reclama ficheiro
# -------------------------
def claim_next_file(worker_id: str):
    """
    Reclama atomicamente UM ficheiro NEW
    """
    try:
        with get_connection() as conn:
            conn.execute("BEGIN")

            row = conn.execute(
                """
                SELECT id, file_path
                FROM file_queue
                WHERE status = 'NEW'
                ORDER BY detected_at
                LIMIT 1
                """
            ).fetchone()

            if not row:
                conn.execute("COMMIT")
                return None

            conn.execute(
                """
                UPDATE file_queue
                SET status = 'PROCESSING',
                    worker_id = ?
                WHERE id = ?
                """,
                (worker_id, row["id"])
            )

            conn.execute("COMMIT")
            return row["file_path"]

    except sqlite3.OperationalError as e:
        # evita crash por lock
        print(f"[QUEUE] DB lock ao reclamar ficheiro: {e}")
        return None


# -------------------------
# Marcar como DONE
# -------------------------
def mark_done(file_path: str):
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE file_queue
            SET status = 'DONE',
                processed_at = ?
            WHERE file_path = ?
            """,
            (datetime.now().isoformat(), file_path)
        )

# -------------------------
# Verificar se ficheiro já existe na fila
# -------------------------
def file_exists(file_path: str) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM file_queue
            WHERE file_path = ?
            """,
            (file_path,)
        ).fetchone()

        return row is not None
# -------------------------
# Marcar como ERROR
# -------------------------
def mark_error(file_path: str):
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE file_queue
            SET status = 'ERROR',
                processed_at = ?
            WHERE file_path = ?
            """,
            (datetime.now().isoformat(), file_path)
        )
