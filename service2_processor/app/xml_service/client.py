import os
import requests


class XmlServiceClient:
    """
    Cliente responsável por enviar o CSV enriquecido
    para o XML Service.
    """

    def __init__(self):
        self.base_url = os.getenv("XML_SERVICE_URL")
        self.mapper_version = os.getenv("XML_MAPPER_VERSION")
        self.webhook_base_url = os.getenv("PD_WEBHOOK_BASE_URL")

        if not self.base_url:
            raise RuntimeError("XML_SERVICE_URL não definida")

        if not self.webhook_base_url:
            raise RuntimeError("PD_WEBHOOK_BASE_URL não definida")

        if not self.mapper_version:
            raise RuntimeError("XML_MAPPER_VERSION não definida")

    def send_csv(self, *, request_id: str, csv_path: str):
        """
        Envia CSV + metadata para o XML Service
        usando multipart/form-data.
        """

        endpoint = f"{self.base_url}/xml/import"

        with open(csv_path, "rb") as csv_file:
            files = {
                "csv_file": csv_file
            }

            data = {
                "request_id": request_id,
                "mapper_version": self.mapper_version,
                # ⚠️ MUITO IMPORTANTE:
                # enviamos APENAS a base URL do PD
                "webhook_url": self.webhook_base_url,
            }

            response = requests.post(
                endpoint,
                files=files,
                data=data,
                timeout=20
            )

        response.raise_for_status()
        return response.status_code
