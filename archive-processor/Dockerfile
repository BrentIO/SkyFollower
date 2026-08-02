FROM python:3.14-slim AS base

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY archive-processor/requirements.txt archive_requirements.txt
RUN pip install --no-cache-dir -r shared_requirements.txt -r archive_requirements.txt

COPY shared/ ./shared/
COPY specs/aws/ ./specs/aws/
COPY archive-processor/ ./archive-processor/

ENV PYTHONPATH=/app

WORKDIR /app/archive-processor

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["python", "main.py"]
