FROM python:3.14-slim@sha256:cae66f2ef0ec51a9891263eeee7f987dacf0a9879e8aa9353d5606e0530619a5

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
