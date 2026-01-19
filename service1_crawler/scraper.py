import requests
from bs4 import BeautifulSoup

URL = "https://worldpopulationreview.com/country-rankings/cost-of-living-by-country"
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def scrape_custo_vida():
    print("[SCRAPER] A iniciar scraping (World Population Review)")

    response = requests.get(URL, headers=HEADERS, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table")
    if table is None:
        raise RuntimeError("Tabela não encontrada na página")

    rows = table.find_all("tr")

    data = []
    idx = 1
    skipped = 0

    for row in rows[1:]:  # ignora cabeçalho
        cols = [c.get_text(strip=True) for c in row.find_all("td")]

        # Estrutura da tabela:
        # cols[1] -> Country
        # cols[2] -> Cost of Living Index
        if len(cols) >= 3:
            pais = cols[1].strip()
            custo = cols[2].replace(",", ".").strip()

            # ignora valores vazios ou inválidos
            if not custo or custo.lower() in {"-", "na", "n/a"}:
                skipped += 1
                continue

            data.append({
                "local_id": f"L{idx:03}",
                "pais": pais,
                "custo_vida_index": custo
            })
            idx += 1

    print(f"[SCRAPER] Scraping concluído")
    print(f"[SCRAPER] Registos válidos: {len(data)}")
    print(f"[SCRAPER] Registos ignorados: {skipped}")

    return data
