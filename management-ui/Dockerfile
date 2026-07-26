FROM python:3.14-slim AS base

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY management-ui/backend/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY message-processor/__init__.py ./message_processor/__init__.py
COPY message-processor/rules_engine.py ./message_processor/rules_engine.py
COPY management-ui/backend/main.py ./main.py

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

# Backend-only for now -- no frontend to serve yet (#15, #16). Once the
# React app exists, this becomes a multi-stage build: a node stage producing
# the static bundle, and nginx proxying /api/* to uvicorn while serving that
# bundle at /.
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
