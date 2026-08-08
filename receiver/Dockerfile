FROM python:3.14-slim AS base

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY receiver/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY receiver/ ./receiver/

# Pinned at /app/healthcheck.py in every image that has one, so all three
# compose files can name the identical HEALTHCHECK command regardless of
# how each image lays its source out underneath.
COPY shared/healthcheck.py ./healthcheck.py

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["python", "-m", "receiver.main"]
