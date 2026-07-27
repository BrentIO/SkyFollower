FROM python:3.14-slim
WORKDIR /app
COPY shared/requirements.txt shared_requirements.txt
COPY runners/gg-2reg-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt
COPY shared/ ./shared/
COPY runners/gg-2reg-registry/ ./gg-2reg-registry/
ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json
RUN touch /app/settings.json
CMD ["python", "gg-2reg-registry/main.py"]
