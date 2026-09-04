FROM python:3.14-slim@sha256:cae66f2ef0ec51a9891263eeee7f987dacf0a9879e8aa9353d5606e0530619a5 AS base

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

# Short commit hash the image was built from; combined with VERSION for
# display (e.g. Home Assistant sw_version) but never part of the image tag.
ARG COMMIT=unknown
ENV GIT_COMMIT=$COMMIT

CMD ["python", "main.py"]
