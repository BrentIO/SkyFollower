FROM python:3.14-slim

WORKDIR /app

COPY shared/requirements.txt shared_requirements.txt
COPY data-runners/ourairports/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt

COPY shared/ ./shared/
COPY data-runners/ourairports/ ./ourairports/

ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json
ENV OVERRIDES_PATH=/app/phonic_overrides.json

RUN touch /app/settings.json
RUN echo '{}' > /app/phonic_overrides.json

CMD ["python", "ourairports/main.py"]
