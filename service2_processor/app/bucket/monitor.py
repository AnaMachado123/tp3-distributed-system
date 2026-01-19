from app.bucket.supabase_client import SupabaseStorageClient
from app.queue.file_queue import enqueue_file, file_exists
from datetime import datetime

class BucketMonitor:
    def __init__(self):
        self.storage = SupabaseStorageClient()

    def _walk_bucket(self, base_path=""):
        files = []

        items = self.storage.list_files(base_path)
        if not items:
            return files

        for item in items:
            name = item["name"]
            full_path = f"{base_path}/{name}" if base_path else name

            # diretório
            if item.get("metadata") is None:
                files.extend(self._walk_bucket(full_path))
            else:
                # 🔒 só CSV
                if not name.lower().endswith(".csv"):
                    continue

                files.append({
                    "path": full_path,
                    "updated_at": item["updated_at"]
                })

        return files

    def poll(self):
        files = self._walk_bucket()

        for f in files:
            file_path = f["path"]

            # 🔑 regra-chave: só entra se ainda não estiver na BD
            if file_exists(file_path):
                continue

            enqueue_file(file_path)
            print(f"[MONITOR] novo ficheiro enfileirado: {file_path}")
