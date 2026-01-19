import csv
import os
import requests
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


# -------------------------
# MAIN DO CRAWLER
# -------------------------

def main():
    # 🔹 aqui assumes que já tens os dados
    # 🔹 isto é só exemplo; a tua lógica real mantém-se
    data = [
        {"local_id": 1, "pais": "Portugal", "custo_vida_index": 67.3},
        {"local_id": 2, "pais": "Espanha", "custo_vida_index": 62.1},
    ]

    bucket_path = write_csv_and_upload(data)

    # 🔔 WAKE-UP DO PROCESSADOR DE DADOS (opcional, seguro)
    pd_wakeup_url = os.getenv("PD_WAKEUP_URL")

    if pd_wakeup_url:
        try:
            requests.post(pd_wakeup_url, timeout=5)
            print("[CRAWLER] Processador de Dados acordado com sucesso")
        except Exception as e:
            print(f"[CRAWLER] Falha ao acordar Processador de Dados: {e}")
    else:
        print("[CRAWLER] PD_WAKEUP_URL não definida (wake-up ignorado)")

    return bucket_path


if __name__ == "__main__":
    main()
