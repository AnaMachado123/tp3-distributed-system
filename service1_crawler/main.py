import os
import time
from dotenv import load_dotenv

from scraper import scrape_custo_vida
from csv_writer import write_csv_and_upload

load_dotenv()

INTERVAL = int(os.getenv("CRAWLER_INTERVAL", 60))
MULTIPLIER = int(os.getenv("DATA_MULTIPLIER", 15))


def expand_data(data: list[dict], factor: int) -> list[dict]:
    expanded = []
    for i in range(factor):
        for item in data:
            new_item = item.copy()
            new_item["local_id"] = f"{item['local_id']}_v{i}"
            expanded.append(new_item)
    return expanded


if __name__ == "__main__":
    while True:
        print("[MAIN] Nova execução do crawler")

        data = scrape_custo_vida()
        data = expand_data(data, MULTIPLIER)

        write_csv_and_upload(data)

        print(f"[MAIN] A aguardar {INTERVAL} segundos...\n")
        time.sleep(INTERVAL)
