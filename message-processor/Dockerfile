FROM python:3.14-slim AS base

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY message-processor/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY message-processor/ ./message_processor/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["python", "-m", "message_processor.main"]
