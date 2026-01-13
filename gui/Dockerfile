FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir flask pyyaml

EXPOSE 8083

CMD ["python", "media_config_gui.py"]
