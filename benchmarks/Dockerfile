FROM ghcr.io/astral-sh/uv:0.12.23 AS uv
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git curl python3 python3-dev python3-venv redis-server \
    postgresql postgresql-client libpq-dev librdkafka-dev build-essential pkg-config \
    libffi-dev libxml2-dev libxslt1-dev libxmlsec1-dev libxmlsec1-openssl \
    libjpeg-dev zlib1g-dev libmagic1 ghostscript poppler-utils qpdf \
    imagemagick tesseract-ocr tesseract-ocr-eng tesseract-ocr-osd \
    && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /uvx /usr/local/bin/
COPY requirements.txt /tmp/requirements.txt
RUN uv venv --python /usr/bin/python3 /opt/orchestrator \
    && uv pip install --python /opt/orchestrator/bin/python -r /tmp/requirements.txt \
    && useradd --uid 1001 --create-home benchmark
ENV PATH="/opt/orchestrator/bin:/usr/lib/postgresql/16/bin:${PATH}" \
    UV_CACHE_DIR=/tmp/uv-cache UV_PYTHON_INSTALL_DIR=/tmp/uv-python \
    PYTHONUNBUFFERED=1
USER benchmark
WORKDIR /workspace
ENTRYPOINT ["python"]
