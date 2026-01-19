import os
import time
import threading

from app.queue.file_queue import (
    claim_next_file,
    mark_error,
    mark_done,
)
from app.bucket.supabase_client import SupabaseStorageClient
from app.processor.csv_reader import stream_csv
from app.processor.enricher import enrich_row
from app.processor.csv_writer import write_enriched_csv


class Worker(threading.Thread):
    def __init__(self, worker_id: str):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.storage = SupabaseStorageClient()

    def run(self):
        print(f"[WORKER {self.worker_id}] iniciado")

        while True:
            file_path = claim_next_file(self.worker_id)

            if not file_path:
                time.sleep(2)
                continue

            print(f"[WORKER {self.worker_id}] a processar {file_path}")

            try:
                self.process_file(file_path)
                mark_done(file_path)
                print(f"[WORKER {self.worker_id}] DONE: {file_path}")

            except Exception as e:
                mark_error(file_path)
                print(f"[WORKER {self.worker_id}] ERROR: {file_path}")
                print(f"[WORKER {self.worker_id}] detalhe: {e}")

    def process_file(self, file_path: str):
        # -------------------------
        # validação
        # -------------------------
        if not file_path.lower().endswith(".csv"):
            raise ValueError("Ficheiro não CSV")

        # -------------------------
        # diretórios locais
        # -------------------------
        download_dir = os.path.join("tmp", "downloads")
        processed_dir = os.path.join("tmp", "processed")

        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)

        local_filename = f"{self.worker_id}_{os.path.basename(file_path)}"
        local_csv = os.path.join(download_dir, local_filename)

        # -------------------------
        # download do CSV original
        # -------------------------
        print(f"[WORKER {self.worker_id}] download -> {local_csv}")
        self.storage.download_file(file_path, local_csv)

        if not os.path.exists(local_csv) or os.path.getsize(local_csv) == 0:
            raise RuntimeError("Download falhou ou CSV vazio")

        # -------------------------
        # leitura em stream + API
        # -------------------------
        enriched_rows = []

        for row in stream_csv(local_csv):
            enriched = enrich_row(row)
            enriched_rows.append(enriched)

        if not enriched_rows:
            raise RuntimeError("CSV não continha dados")

        # -------------------------
        # gerar CSV enriquecido
        # -------------------------
        output_filename = f"enriched_{local_filename}"
        output_csv = os.path.join(processed_dir, output_filename)

        write_enriched_csv(enriched_rows, output_csv)

        print(
            f"[WORKER {self.worker_id}] "
            f"CSV enriquecido gerado ({len(enriched_rows)} linhas)"
        )

        # -------------------------
        # upload para bucket de saída
        # -------------------------
        remote_path = f"{os.path.dirname(file_path)}/{output_filename}".replace("\\", "/")


        print(
            f"[WORKER {self.worker_id}] upload -> "
            f"{self.storage.output_bucket}/{remote_path}"
        )

        self.storage.upload_processed_file(
            local_path=output_csv,
            remote_path=remote_path
        )

        print(
            f"[WORKER {self.worker_id}] "
            f"CSV enviado para bucket processed"
        )
