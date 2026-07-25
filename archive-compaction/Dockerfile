FROM python:3.14-slim

WORKDIR /app

COPY archive-compaction/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY archive-compaction/ ./archive-compaction/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

RUN touch /app/settings.json

CMD ["python", "archive-compaction/main.py"]
