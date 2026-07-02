FROM python:3.11-slim
WORKDIR /app
COPY shared/requirements.txt shared_requirements.txt
COPY data-runners/ge-gcaa/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt
COPY shared/ ./shared/
COPY data-runners/ge-gcaa/ ./ge-gcaa/
ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json
RUN touch /app/settings.json
CMD ["python", "ge-gcaa/main.py"]
