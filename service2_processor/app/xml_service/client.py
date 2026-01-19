import os
import time
import requests
from app.config import settings


class XmlServiceClient:
    """
    Cliente REST para o XML Service.
    Envia multipart/form-data com:
      - request_id
      - mapper_version
      - webhook_url
      - enriched_csv (ficheiro)
    """

    def __init__(self):
        if not settings.XML_SERVICE_URL:
            raise RuntimeError("XML_SERVICE_URL não definido no .env")
        if not settings.WEBHOOK_URL:
            raise RuntimeError("WEBHOOK_URL não definido no .env")

        self.base_url = settings.XML_SERVICE_URL.rstrip("/")
        self.ingest_path = settings.XML_SERVICE_INGEST_PATH if settings.XML_SERVICE_INGEST_PATH.startswith("/") else f"/{settings.XML_SERVICE_INGEST_PATH}"
        self.timeout = settings.HTTP_TIMEOUT_SECONDS
        self.retries = settings.HTTP_RETRY_COUNT

    def send_enriched_csv(self, *, request_id: str, enriched_csv_path: str) -> dict:
        """
        Retorna um dict com a resposta do XML Service (JSON esperado).
        Não apaga ficheiros aqui.
        """
        if not os.path.exists(enriched_csv_path):
            raise FileNotFoundError(f"CSV enriquecido não encontrado: {enriched_csv_path}")

        url = f"{self.base_url}{self.ingest_path}"

        data = {
            "request_id": request_id,
            "mapper_version": settings.MAPPER_VERSION,
            "webhook_url": settings.WEBHOOK_URL,
        }

        # multipart/form-data
        with open(enriched_csv_path, "rb") as f:
            files = {
                "file": (os.path.basename(enriched_csv_path), f, "text/csv")
            }

            last_exc = None
            for attempt in range(1, self.retries + 1):
                try:
                    resp = requests.post(url, data=data, files=files, timeout=self.timeout)
                    # 2xx = aceitou
                    if 200 <= resp.status_code < 300:
                        # esperamos JSON (o XML Service deve responder em JSON)
                        try:
                            return resp.json()
                        except Exception:
                            # fallback: devolve texto
                            return {"status": "ok", "raw": resp.text}

                    # Erros 4xx/5xx: devolve conteúdo p/ debug
                    raise RuntimeError(f"XML Service respondeu {resp.status_code}: {resp.text}")

                except Exception as e:
                    last_exc = e
                    # backoff simples
                    if attempt < self.retries:
                        time.sleep(1.0 * attempt)
                        continue
                    raise last_exc
