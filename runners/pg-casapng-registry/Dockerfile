FROM python:3.15-slim@sha256:2da676046630d7bf14c6764ae1143dc7fa0377a53f94bf5a642f0b214aca5bce
WORKDIR /app
COPY shared/requirements.txt shared_requirements.txt
COPY runners/pg-casapng-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt
COPY shared/ ./shared/
COPY runners/pg-casapng-registry/ ./pg-casapng-registry/
ENV PYTHONPATH=/app
# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
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

CMD ["python", "pg-casapng-registry/main.py"]
