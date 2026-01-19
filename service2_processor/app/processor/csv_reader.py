import csv

def stream_csv(file_path: str):
    """
    Lê um CSV linha a linha (streaming).
    Não carrega o ficheiro inteiro em memória.
    """
    with open(file_path, mode="r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            yield {
                "local_id": row.get("local_id"),
                "pais": row.get("pais"),
                "custo_vida_index": row.get("custo_vida_index")
            }
