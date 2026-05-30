FROM python:3.11-slim AS base

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY processor/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY processor/ ./processor/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

CMD ["python", "-m", "processor.main"]
