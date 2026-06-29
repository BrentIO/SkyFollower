FROM python:3.11-slim
WORKDIR /app
COPY shared/requirements.txt shared_requirements.txt
COPY data-runners/bz-bdca/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt
COPY shared/ ./shared/
COPY data-runners/bz-bdca/ ./bz-bdca/
ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json
RUN touch /app/settings.json
CMD ["python", "bz-bdca/main.py"]
