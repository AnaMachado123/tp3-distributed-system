import uuid
from app.external_api.countries_api import get_country_info

# cache em memória por execução do worker
_country_cache = {}


def enrich_row(row: dict) -> dict:
    pais = row.get("pais")

    if not pais:
        raise ValueError("Campo 'pais' em falta no CSV")

    # chama a API apenas uma vez por país
    if pais not in _country_cache:
        _country_cache[pais] = get_country_info(pais)

    api_data = _country_cache[pais]

    enriched = dict(row)
    enriched.update({
        "request_id": str(uuid.uuid4()),
        "region": api_data.get("region"),
        "subregion": api_data.get("subregion"),
        "population": api_data.get("population"),
    })

    return enriched
