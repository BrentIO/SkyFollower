FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6

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
