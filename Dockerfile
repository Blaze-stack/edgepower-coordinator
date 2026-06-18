FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    EDGEPOWER_DB_PATH=/data/edgepower-coordinator.sqlite3

WORKDIR /app

COPY pyproject.toml README.md ./
COPY edgepower_coordinator ./edgepower_coordinator

RUN pip install --no-cache-dir .

RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /data /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "edgepower_coordinator.app:app", "--host", "0.0.0.0", "--port", "8000"]
