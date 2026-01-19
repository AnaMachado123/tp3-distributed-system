import requests

API_URL = "https://restcountries.com/v3.1/name/{}"
TIMEOUT = 5  # segundos


def get_country_info(country_name: str) -> dict:
    if not country_name:
        raise ValueError("Nome do país vazio")

    url = API_URL.format(country_name)

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
