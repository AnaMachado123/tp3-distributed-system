FROM python:3.10-slim

# evitar logs buffered
ENV PYTHONUNBUFFERED=1

# diretório de trabalho
WORKDIR /app

# dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# código
COPY . .

# comando de arranque
CMD ["python", "-m", "app.main"]
