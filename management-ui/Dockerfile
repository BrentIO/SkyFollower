FROM node:22-slim AS frontend-build

WORKDIR /frontend

COPY management-ui/frontend/package*.json ./
RUN npm ci

COPY management-ui/frontend/ ./

# VITE_VERSION must be a real env var before the build runs -- Vite bakes
# import.meta.env.VITE_* values into the bundle at build time, not runtime.
ARG VERSION=dev
ENV VITE_VERSION=$VERSION
RUN npm run build

FROM python:3.14-slim AS base

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends nginx \
    && rm -rf /var/lib/apt/lists/*

COPY shared/requirements.txt shared_requirements.txt
COPY management-ui/backend/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY specs/aws/ ./specs/aws/
COPY message-processor/__init__.py ./message_processor/__init__.py
COPY message-processor/rules_engine.py ./message_processor/rules_engine.py
COPY management-ui/backend/main.py ./main.py

COPY --from=frontend-build /frontend/dist /usr/share/nginx/html
COPY management-ui/nginx.conf /etc/nginx/conf.d/default.conf
COPY management-ui/entrypoint.sh /entrypoint.sh
RUN rm -f /etc/nginx/sites-enabled/default && chmod +x /entrypoint.sh

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

EXPOSE 80

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["/entrypoint.sh"]
