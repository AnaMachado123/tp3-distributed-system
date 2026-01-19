import time
import threading

from app.bucket.monitor import BucketMonitor
from app.processor.worker import Worker
from app.queue.db import init_db
from app.http_server import start_http_server


def start_monitor():
    monitor = BucketMonitor()
    while True:
        monitor.poll()
        time.sleep(5)


def main():
    print("[SERVICE 2] Processador de Dados iniciado")

    init_db()
    print("[SERVICE 2] DB inicializada")

    # HTTP server (para Render)
    http_thread = threading.Thread(
        target=start_http_server,
        daemon=True
    )
    http_thread.start()
    print("[SERVICE 2] HTTP server iniciado")

    # Monitor
    monitor_thread = threading.Thread(
        target=start_monitor,
        daemon=True
    )
    monitor_thread.start()
    print("[SERVICE 2] Monitor iniciado")

    # Workers
    for i in range(1):
        w = Worker(worker_id=f"worker-{i+1}")
        w.start()

    print("[SERVICE 2] Worker ativo")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()



def start_monitor():
    monitor = BucketMonitor()
    while True:
        monitor.poll()
        time.sleep(5)


def main():
    print("[SERVICE 2] Processador de Dados iniciado")

    init_db()
    print("[SERVICE 2] DB inicializada")

    # HTTP server (para Render)
    http_thread = threading.Thread(
        target=start_http_server,
        daemon=True
    )
    http_thread.start()
    print("[SERVICE 2] HTTP server iniciado")

    # Monitor
    monitor_thread = threading.Thread(
        target=start_monitor,
        daemon=True
    )
    monitor_thread.start()
    print("[SERVICE 2] Monitor iniciado")

    # Workers
    for i in range(1):
        w = Worker(worker_id=f"worker-{i+1}")
        w.start()

    print("[SERVICE 2] Worker ativo")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
