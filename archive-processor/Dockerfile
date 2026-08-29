FROM python:3.14-slim@sha256:cae66f2ef0ec51a9891263eeee7f987dacf0a9879e8aa9353d5606e0530619a5 AS base

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY archive-processor/requirements.txt archive_requirements.txt
RUN pip install --no-cache-dir -r shared_requirements.txt -r archive_requirements.txt

COPY shared/ ./shared/
COPY archive-processor/ ./archive-processor/

# Pinned at /app/healthcheck.py in every image that has one, so all three
# compose files can name the identical HEALTHCHECK command regardless of
# how each image lays its source out underneath.
COPY shared/healthcheck.py ./healthcheck.py

ENV PYTHONPATH=/app

WORKDIR /app/archive-processor

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["python", "main.py"]
