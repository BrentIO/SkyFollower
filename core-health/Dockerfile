FROM python:3.14-slim AS base

WORKDIR /app

COPY core-health/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY core-health/ ./core-health/

# Pinned at /app/healthcheck.py in every image that has one, so all compose
# files can name the identical HEALTHCHECK command regardless of how each
# image lays its source out underneath.
COPY shared/healthcheck.py ./healthcheck.py

ENV PYTHONPATH=/app

WORKDIR /app/core-health

# VERSION changes every release; placed last so it only busts this cheap
# final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["python", "main.py"]
