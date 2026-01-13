FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p logs

CMD ["sh", "-c", "while true; do python torrent_series_retriever.py; sleep 600; done"]
