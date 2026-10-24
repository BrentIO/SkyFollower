FROM python:3.14-slim@sha256:a2b82f3c48559aa0a8446d9af49826b6e2b2016f4cd2afabfe6013ec53729170

WORKDIR /app

COPY tools/legacy-migration/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY tools/legacy-migration/ ./tools/legacy-migration/

ENV PYTHONPATH=/app

WORKDIR /app/tools/legacy-migration

# Baked in so `docker compose run --rm producer/worker/verify` needs no
# entrypoint of its own -- each service in docker-compose.legacy-migration.yaml
# overrides ENTRYPOINT with its role already appended (e.g.
# ["python", "main.py", "produce"]), and `up`/`run` supplies only the
# trailing flags (--start-date/--end-date) as the command. `work` is this
# image's own default so a bare `docker compose up --scale worker=N`
# starts the worker service with no per-service override at all.
ENTRYPOINT ["python", "main.py"]
CMD ["work"]
