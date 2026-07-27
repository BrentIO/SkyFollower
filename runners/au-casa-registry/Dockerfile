FROM python:3.14-slim

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY runners/au-casa-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY runners/au-casa-registry/ ./au-casa-registry/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

RUN touch /app/settings.json

CMD ["python", "au-casa-registry/main.py"]
