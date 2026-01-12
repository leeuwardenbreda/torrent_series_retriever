FROM python:3.11-slim-bullseye

# systeemvereisten
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# werkdir
WORKDIR /app

# copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy app
COPY . .

# expose web UI port
EXPOSE 8000

# default command: start scheduler + webapp
CMD ["python", "main.py"]
