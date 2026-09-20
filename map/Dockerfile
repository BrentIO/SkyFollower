# Builds the frontend SPA (map/frontend/, a standalone Vite project --
# see map/README.md's "Frontend" section) so the final image can serve it
# itself under /map (map/main.py's _SPAStaticFiles mount) -- one container,
# no separate static-file server. Mirrors management-ui/Dockerfile's
# node-build-stage-then-python-stage structure, but the built assets land
# inside the Python stage's own app tree (map/frontend/dist/) rather than
# an nginx html root, since this image serves them itself via FastAPI
# rather than nginx.
FROM node:26-slim@sha256:14bf3eac4bf209d906d3c41256597d3ab1f926b2e93a79e9bdfe1efd32454239 AS frontend-build

WORKDIR /frontend

COPY map/frontend/package*.json ./

# `npm install --package-lock-only` before `npm ci` heals a known class of
# lockfile corruption: an npm CLI bug (npm/cli#8535, fixed by npm/cli#8645 in
# npm 11.6.3) miscalculates peer+optional dependency flags and, on `npm
# install`, prunes @tailwindcss/oxide-wasm32-wasi's bundled optional
# sub-dependencies (@emnapi/*, tslib) from package-lock.json on platforms
# that don't need that variant (e.g. macOS). If that pruned lockfile is
# committed or just sitting in a worktree, `npm ci` below fails with a
# misleading dump of workspaces/--install-links usage instead of the real
# "Missing: @emnapi/core@... from lock file" reason. Regenerating the lock
# here, with this image's own unaffected npm, is a no-op against an
# already-correct lockfile and heals a corrupted one -- so this stage never
# depends on the npm version a contributor's machine happened to run `npm
# install` with. See map/README.md's "Frontend" section and GitHub issue
# #1872 for the full writeup.
RUN npm install --package-lock-only
RUN npm ci

COPY map/frontend/ ./

# VITE_VERSION must be a real env var before the build runs -- Vite bakes
# import.meta.env.VITE_* values into the bundle at build time, not runtime.
ARG VERSION=dev
ENV VITE_VERSION=$VERSION

# Short commit hash the image was built from; combined with VITE_VERSION for
# display in the aircraft list footer but never part of the image tag.
ARG COMMIT=unknown
ENV VITE_COMMIT=$COMMIT
RUN npm run build

FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6 AS base

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY map/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY map/ ./map/

# Built frontend assets, read by map/main.py's _FRONTEND_DIST_DIR /
# _SPAStaticFiles mount at /app/map/frontend/dist -- same relative
# location `npm run build` produces outside Docker (map/frontend/dist/),
# so the mount path needs no Docker-specific special-casing.
COPY --from=frontend-build /frontend/dist ./map/frontend/dist

# Pinned at /app/healthcheck.py in every image that has one, so all compose
# files can name the identical HEALTHCHECK command regardless of how each
# image lays its source out underneath.
COPY shared/healthcheck.py ./healthcheck.py

ENV PYTHONPATH=/app

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

# Short commit hash the image was built from; combined with VERSION for
# display but never part of the image tag.
ARG COMMIT=unknown
ENV GIT_COMMIT=$COMMIT

# Bare "skyfollower-<component>" image name this build published to GHCR
# (the repository shared/version_check.py's get_latest_ghcr_tag() looks
# up under ghcr.io/brentio/). Read at runtime by
# shared/mqtt_register.py's publish_register() so core-health never has
# to infer a component's image name from what it calls itself.
ARG IMAGE=unknown
ENV COMPONENT_IMAGE=$IMAGE

CMD ["python", "-m", "map.main"]
