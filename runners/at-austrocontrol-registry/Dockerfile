FROM python:3.14-slim@sha256:cae66f2ef0ec51a9891263eeee7f987dacf0a9879e8aa9353d5606e0530619a5

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY runners/at-austrocontrol-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY runners/at-austrocontrol-registry/ ./at-austrocontrol-registry/

ENV PYTHONPATH=/app

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

CMD ["python", "at-austrocontrol-registry/main.py"]
