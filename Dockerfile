FROM python:3.11

WORKDIR /app

# mediainfo: /mediainfo report page. ffmpeg: audio-track / subtitle switching in the web player.
RUN apt-get update && apt-get install -y mediainfo ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
