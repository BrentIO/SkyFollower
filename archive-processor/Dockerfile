FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6 AS base

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

# Short commit hash the image was built from; combined with VERSION for
# display (e.g. Home Assistant sw_version) but never part of the image tag.
ARG COMMIT=unknown
ENV GIT_COMMIT=$COMMIT

CMD ["python", "main.py"]
