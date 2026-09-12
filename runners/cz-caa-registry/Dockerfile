FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6
WORKDIR /app
COPY shared/requirements.txt shared_requirements.txt
COPY runners/cz-caa-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt
COPY shared/ ./shared/
COPY runners/cz-caa-registry/ ./cz-caa-registry/
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

CMD ["python", "cz-caa-registry/main.py"]
