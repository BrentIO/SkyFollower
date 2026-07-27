FROM python:3.14-slim
WORKDIR /app
COPY shared/requirements.txt shared_requirements.txt
COPY runners/lu-dac-registry/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt
COPY shared/ ./shared/
COPY runners/lu-dac-registry/ ./lu-dac-registry/
ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json
RUN touch /app/settings.json
CMD ["python", "lu-dac-registry/main.py"]
