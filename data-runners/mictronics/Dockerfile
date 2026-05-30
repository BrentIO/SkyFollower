FROM python:3.11-slim

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY data-runners/mictronics/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY data-runners/mictronics/ ./mictronics/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

CMD ["python", "mictronics/main.py"]
