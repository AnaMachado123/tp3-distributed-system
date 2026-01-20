import os
from flask import Flask, request, jsonify

# ✅ estado partilhado com o Worker (sem circular import)
from app.state.request_files import APPROVED_REQUESTS

# 🔐 Token simples para autenticação do webhook
WEBHOOK_TOKEN = os.getenv("PD_WEBHOOK_TOKEN")


def start_http_server():
    app = Flask(__name__)

    @app.route("/")
    def root():
        return {"service": "processor", "status": "running"}, 200

    @app.route("/health")
    def health():
        return {"status": "ok"}, 200

    # 🔔 Webhook REST (XML Service → Processador de Dados)
    @app.route("/webhook/xml-status", methods=["POST"])
    def xml_status_webhook():
        # ✅ validação do token
        if not WEBHOOK_TOKEN:
            return jsonify({"error": "server_misconfigured", "detail": "PD_WEBHOOK_TOKEN not set"}), 500

        token = request.headers.get("X-WEBHOOK-TOKEN")
        if not token or token != WEBHOOK_TOKEN:
            return jsonify({"error": "unauthorized"}), 401

        # ✅ parse JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"error": "invalid_json"}), 400

        request_id = data.get("request_id")
        status = data.get("status")
        xml_document_id = data.get("xml_document_id")

        if not request_id or not status:
            return jsonify({"error": "missing_fields"}), 400

        print("[WEBHOOK] XML status recebido:")
        print("  request_id:", request_id)
        print("  status:", status)
        print("  xml_document_id:", xml_document_id)

        # ✅ só sinaliza — o Worker fará o cleanup no próprio thread
        if status == "OK":
            APPROVED_REQUESTS.add(request_id)
            print(f"[WEBHOOK] request marcado como aprovado: {request_id}")

        return jsonify({"received": True}), 200

    app.run(host="0.0.0.0", port=8080)
