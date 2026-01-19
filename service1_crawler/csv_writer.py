import csv
import os
from datetime import datetime
from supabase import create_client


def write_csv_and_upload(data: list[dict]) -> str:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    bucket_name = os.getenv("SUPABASE_BUCKET", "custo-vida")

    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL/SUPABASE_KEY não estão definidos no .env")

    supabase = create_client(supabase_url, supabase_key)

    now = datetime.now()
    filename = f"custo_vida_{now.strftime('%Y%m%d_%H%M%S')}.csv"

    # caminho hierárquico (árvore de banda)
    bucket_path = f"{now.year}/{now.month:02}/{now.day:02}/{filename}"

    # 1) cria CSV local temporário
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["local_id", "pais", "custo_vida_index"]
        )
        writer.writeheader()
        writer.writerows(data)

    # 2) upload para o bucket (sem sobrescrita)
    with open(filename, "rb") as f:
        supabase.storage.from_(bucket_name).upload(
            path=bucket_path,
            file=f,
            file_options={
                "content-type": "text/csv",
                "upsert": "false"
            },
        )

    # 3) remove ficheiro local
    os.remove(filename)

    print(f"[BUCKET] CSV enviado: {bucket_name}/{bucket_path}")
    return bucket_path
