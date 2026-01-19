from flask import Flask

def start_http_server():
    app = Flask(__name__)

    @app.route("/")
    def root():
        return {"service": "processor", "status": "running"}, 200

    @app.route("/health")
    def health():
        return {"status": "ok"}, 200

    app.run(host="0.0.0.0", port=8080)
