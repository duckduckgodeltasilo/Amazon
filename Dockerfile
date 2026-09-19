FROM python:3.11-slim-bookworm

# System dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Requirements pehle copy karo — caching ke liye
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright ke liye Chromium + system deps (fonts, libnss3, etc.) install
RUN playwright install --with-deps chromium

# Bot file copy karo
COPY bot.py .

# Restart hone pe bhi data safe rahe
ENV PYTHONUNBUFFERED=1

CMD ["python", "bot.py"]
