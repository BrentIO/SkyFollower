FROM python:3.15-slim@sha256:2da676046630d7bf14c6764ae1143dc7fa0377a53f94bf5a642f0b214aca5bce AS base

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

# Bare "skyfollower-<component>" image name this build published to GHCR
# (the repository shared/version_check.py's get_latest_ghcr_tag() looks
# up under ghcr.io/brentio/). Read at runtime by
# shared/mqtt_register.py's publish_register() so core-health never has
# to infer a component's image name from what it calls itself.
ARG IMAGE=unknown
ENV COMPONENT_IMAGE=$IMAGE

CMD ["python", "main.py"]
