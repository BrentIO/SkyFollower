FROM python:3.11-slim
WORKDIR /app
COPY shared/requirements.txt shared_requirements.txt
COPY data-runners/tc-caa/requirements.txt .
RUN pip install --no-cache-dir -r shared_requirements.txt -r requirements.txt
COPY shared/ ./shared/
COPY data-runners/tc-caa/ ./tc-caa/
ENV PYTHONPATH=/app
ENV SETTINGS_PATH=/app/settings.json
RUN touch /app/settings.json
CMD ["python", "tc-caa/main.py"]
