FROM python:3.14-slim
WORKDIR /app
COPY shared/requirements.txt shared_requirements.txt
COPY runners/tg-anac-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt
COPY shared/ ./shared/
COPY runners/tg-anac-registry/ ./tg-anac-registry/
ENV PYTHONPATH=/app
# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["python", "tg-anac-registry/main.py"]
