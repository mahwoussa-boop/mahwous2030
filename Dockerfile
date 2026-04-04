FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install OS-level dependencies for building and Playwright
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY merged/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# FATAL BARRIER FIX: Install Playwright browsers with system dependencies
RUN playwright install --with-deps chromium

COPY merged/ .

# Secure the absolute data path
RUN mkdir -p /app/data && chmod 777 /app/data

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
