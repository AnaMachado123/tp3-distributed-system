import csv
import os

FIELDNAMES = [
    "local_id",
    "pais",
    "custo_vida_index",
    "request_id",
    "region",
    "subregion",
    "population",
]

def write_enriched_csv(rows, output_path, append=False, header_only=False):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    mode = "a" if append else "w"

    with open(output_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDNAMES,
            extrasaction="ignore"  
        )

        if f.tell() == 0:
            writer.writeheader()

        if not header_only:
            for row in rows:
                writer.writerow(row)
