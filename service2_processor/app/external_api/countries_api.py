import time
import requests

API_URL = "https://restcountries.com/v3.1/name/{}"
TIMEOUT = 20
MAX_RETRIES = 3


def get_country_info(country_name: str) -> dict:
    if not country_name:
        raise ValueError("Nome do país vazio")

    url = API_URL.format(country_name)

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=TIMEOUT)

            if response.status_code != 200:
                raise RuntimeError(
                    f"API error {response.status_code} para país {country_name}"
                )

            data = response.json()

            if not data or not isinstance(data, list):
                raise RuntimeError("Resposta inesperada da API")

            country = data[0]

            return {
                "region": country.get("region"),
                "subregion": country.get("subregion"),
                "population": country.get("population"),
            }

        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(2)
            else:
                raise RuntimeError(
                    f"Falha na API RestCountries após {MAX_RETRIES} tentativas"
                ) from last_error
