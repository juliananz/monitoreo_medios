FROM python:3.11-slim

WORKDIR /app

# libgomp1 is required by PyTorch CPU
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install PyTorch CPU-only first to avoid downloading the 2 GB CUDA wheels.
# PEP 440: torch==2.9.1+cpu satisfies the constraint torch==2.9.1, so
# the subsequent pip install -r requirements.txt will skip torch.
RUN pip install --no-cache-dir \
        torch==2.9.1 \
        --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# PYTHONPATH lets Python resolve top-level packages (analisis, config, etc.)
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
