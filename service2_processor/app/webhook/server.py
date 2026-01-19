from flask import Flask, request, jsonify
import os

from app.queue.file_queue import mark_done
from app.bucket.supabase_client import SupabaseStorageClient

app = Flask(__name__)
storage = SupabaseStorageClient()


@app.route("/webhook/xml-confirmation", methods=["POST"])
def xml_confirmation():
    """
    Endpoint chamado pelo XML Service após persistência bem-sucedida do XML.
    """
    data = request.get_json(silent=True)

    if not data:
        return jsonify({"error": "JSON inválido"}), 400

    request_id = data.get("request_id")
    file_path = data.get("file_path")

    if not request_id or not file_path:
        return jsonify({"error": "request_id ou file_path em falta"}), 400

    try:
        # 1️⃣ marcar ficheiro como DONE
        mark_done(file_path)

        # 2️⃣ apagar CSV original do bucket
        storage.delete_file(file_path)

        # 3️⃣ limpeza de temporários (opcional mas correto)
        _cleanup_temp_files(request_id)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _cleanup_temp_files(request_id: str):
    enriched_dir = "tmp/enriched"
    if not os.path.isdir(enriched_dir):
        return

    for f in os.listdir(enriched_dir):
        if request_id in f:
            os.remove(os.path.join(enriched_dir, f))
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7000)