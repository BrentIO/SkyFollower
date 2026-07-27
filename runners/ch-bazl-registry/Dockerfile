FROM python:3.14-slim

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY runners/ch-bazl-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY runners/ch-bazl-registry/ ./ch-bazl-registry/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

RUN touch /app/settings.json

CMD ["python", "ch-bazl-registry/main.py"]
