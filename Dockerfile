FROM python:3.11

WORKDIR /app

# mediainfo: the /mediainfo report and the quick summary in the web player.
RUN apt-get update && apt-get install -y --no-install-recommends mediainfo \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
