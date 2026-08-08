FROM python:3.14-slim

WORKDIR /app

COPY archive-compaction/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY specs/aws/ ./specs/aws/
COPY archive-compaction/ ./archive-compaction/

ENV PYTHONPATH=/app

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["python", "archive-compaction/main.py"]
