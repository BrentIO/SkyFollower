FROM python:3.14-slim@sha256:cae66f2ef0ec51a9891263eeee7f987dacf0a9879e8aa9353d5606e0530619a5

WORKDIR /app

COPY aws-setup/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY specs/aws/ ./specs/aws/
COPY aws-setup/ ./aws-setup/

ENV PYTHONPATH=/app

# VERSION changes every release; placed last so it only busts this
# cheap final layer, not the pip-install layer above.
ARG VERSION=dev
ENV VERSION=$VERSION

ENTRYPOINT ["python", "aws-setup/main.py"]
