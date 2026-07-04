FROM python:3.14-slim AS base

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY receiver/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY receiver/ ./receiver/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

CMD ["python", "-m", "receiver.main"]
