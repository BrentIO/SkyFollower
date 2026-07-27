FROM python:3.14-slim

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY runners/at-austrocontrol-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY runners/at-austrocontrol-registry/ ./at-austrocontrol-registry/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

RUN touch /app/settings.json

CMD ["python", "at-austrocontrol-registry/main.py"]
