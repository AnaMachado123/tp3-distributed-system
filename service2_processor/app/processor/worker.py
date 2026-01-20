import os
import time
import threading
import uuid

from app.queue.file_queue import (
    claim_next_file,
    mark_error,
)
from app.bucket.supabase_client import SupabaseStorageClient
from app.processor.csv_reader import stream_csv
from app.processor.enricher import enrich_row
from app.processor.csv_writer import write_enriched_csv
from app.xml_service.client import XmlServiceClient

# 📌 estado partilhado com o webhook
from app.state.request_files import REQUEST_FILES, APPROVED_REQUESTS


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
                request_id = self.process_file(file_path)

                print(
                    f"[WORKER {self.worker_id}] "
                    f"enviado para XML Service: {file_path}"
                )

                # 🔁 espera passiva pela confirmação do webhook
                self.wait_and_cleanup(request_id)

            except Exception as e:
                mark_error(file_path)
                print(f"[WORKER {self.worker_id}] ERROR: {file_path}")
                print(f"[WORKER {self.worker_id}] detalhe: {e}")

    # -------------------------------------------------
    # processamento principal
    # -------------------------------------------------
    def process_file(self, file_path: str) -> str:
        if not file_path.lower().endswith(".csv"):
            raise ValueError("Ficheiro não CSV")

        download_dir = os.path.join("tmp", "downloads")
        processed_dir = os.path.join("tmp", "processed")

        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)

        local_filename = f"{self.worker_id}_{os.path.basename(file_path)}"
        local_csv = os.path.join(download_dir, local_filename)

        # download
        print(f"[WORKER {self.worker_id}] download -> {local_csv}")
        self.storage.download_file(file_path, local_csv)

        if not os.path.exists(local_csv) or os.path.getsize(local_csv) == 0:
            raise RuntimeError("Download falhou ou CSV vazio")

        # enriquecimento
        enriched_rows = []
        for row in stream_csv(local_csv):
            enriched_rows.append(enrich_row(row))

        if not enriched_rows:
            raise RuntimeError("CSV não continha dados")

        # CSV enriquecido
        output_filename = f"enriched_{local_filename}"
        output_csv = os.path.join(processed_dir, output_filename)

        write_enriched_csv(enriched_rows, output_csv)

        print(
            f"[WORKER {self.worker_id}] "
            f"CSV enriquecido gerado ({len(enriched_rows)} linhas)"
        )

        # upload
        remote_path = (
            f"{os.path.dirname(file_path)}/{output_filename}"
            .replace("\\", "/")
        )

        print(
            f"[WORKER {self.worker_id}] upload -> "
            f"{self.storage.output_bucket}/{remote_path}"
        )

        try:
            self.storage.upload_processed_file(
                local_path=output_csv,
                remote_path=remote_path
            )
            print(
                f"[WORKER {self.worker_id}] "
                f"CSV enviado para bucket processed"
            )
        except Exception as e:
            if "Duplicate" in str(e) or "409" in str(e):
                print(
                    f"[WORKER {self.worker_id}] "
                    f"CSV já existe no bucket processed (ignorado)"
                )
            else:
                raise

        # envio ao XML Service
        request_id = str(uuid.uuid4())
        xml_client = XmlServiceClient()

        # 📌 registo exato dos ficheiros deste request
        REQUEST_FILES[request_id] = {
            "downloaded_csv": local_csv,
            "enriched_csv": output_csv,
        }

        xml_client.send_csv(
            request_id=request_id,
            csv_path=output_csv
        )

        print(
            f"[WORKER {self.worker_id}] "
            f"CSV enviado ao XML Service (request_id={request_id})"
        )

        return request_id

    # -------------------------------------------------
    # limpeza segura (executada pelo próprio worker)
    # -------------------------------------------------
    def wait_and_cleanup(self, request_id: str):
        print(
            f"[WORKER {self.worker_id}] "
            f"à espera de confirmação OK (request_id={request_id})"
        )

        while request_id not in APPROVED_REQUESTS:
            time.sleep(0.5)

        files = REQUEST_FILES.pop(request_id, {})

        for label, path in files.items():
            try:
                if os.path.exists(path):
                    os.remove(path)
                    print(
                        f"[WORKER {self.worker_id}] "
                        f"cleanup {label}: {path}"
                    )
            except Exception as e:
                print(
                    f"[WORKER {self.worker_id}] "
                    f"erro ao remover {label} ({path}): {e}"
                )

        APPROVED_REQUESTS.remove(request_id)
        print(
            f"[WORKER {self.worker_id}] "
            f"cleanup concluído (request_id={request_id})"
        )
