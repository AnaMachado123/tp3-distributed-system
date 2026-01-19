from app.queue.db import get_connection

def show_queue():
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, file_path, status, detected_at
            FROM file_queue
            ORDER BY detected_at
        """).fetchall()

    print("\n--- FILE QUEUE ---")
    for row in rows:
        print(dict(row))

if __name__ == "__main__":
    show_queue()
