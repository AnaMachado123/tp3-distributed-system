import os
from supabase import create_client
from app.config import settings


class SupabaseStorageClient:
    def __init__(self):
        self.client = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_KEY
        )

        # bucket de entrada (crawler → processor)
        self.input_bucket = settings.SUPABASE_BUCKET

        # bucket de saída (processor → XML service)
        self.output_bucket = getattr(
            settings,
            "SUPABASE_PROCESSED_BUCKET",
            None
        )

    # -------------------------
    # LISTAR ficheiros (entrada)
    # -------------------------
    def list_files(self, path=""):
        return self.client.storage.from_(self.input_bucket).list(path)

    # -------------------------
    # DOWNLOAD (entrada)
    # -------------------------
    def download_file(self, path: str, local_path: str):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        data = self.client.storage.from_(self.input_bucket).download(path)

        if not data:
            raise RuntimeError(f"Download falhou ou ficheiro vazio: {path}")

        with open(local_path, "wb") as f:
            f.write(data)

    # -------------------------
    # UPLOAD (saída)
    # -------------------------
    def upload_processed_file(self, local_path: str, remote_path: str):
        if not self.output_bucket:
            raise RuntimeError("SUPABASE_PROCESSED_BUCKET não configurado")

        if not os.path.exists(local_path):
            raise FileNotFoundError(local_path)
        with open(local_path, "rb") as f:
            self.client.storage.from_(self.output_bucket).upload(
                remote_path,
                f
            )

    # -------------------------
    # DELETE (entrada ou saída)
    # -------------------------
    def delete_file(self, path: str, processed: bool = False):
        bucket = (
            self.output_bucket if processed
            else self.input_bucket
        )
        self.client.storage.from_(bucket).remove([path])
