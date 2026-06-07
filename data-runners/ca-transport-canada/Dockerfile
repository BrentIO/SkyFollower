FROM python:3.11-slim

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY data-runners/ca-transport-canada/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY data-runners/ca-transport-canada/ ./ca-transport-canada/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

CMD ["python", "ca-transport-canada/main.py"]
