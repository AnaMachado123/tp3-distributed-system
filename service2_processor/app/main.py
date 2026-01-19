import time
import threading

from app.bucket.monitor import BucketMonitor
from app.processor.worker import Worker
from app.queue.db import init_db


def start_monitor():
    monitor = BucketMonitor()
    while True:
        monitor.poll()
        time.sleep(5)  # polling a cada 5s


def main():
    print("[SERVICE 2] Processador de Dados iniciado")

    init_db()
    print("[SERVICE 2] DB inicializada")

    # MONITOR (1 thread)
    monitor_thread = threading.Thread(
        target=start_monitor,
        daemon=True
    )
    monitor_thread.start()
    print("[SERVICE 2] Monitor iniciado")

    # WORKER ÚNICO (processamento sequencial)
    worker = Worker(worker_id="worker-1")
    worker.start()
    print("[SERVICE 2] 1 worker ativo")

    # manter serviço vivo
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
