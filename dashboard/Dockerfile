FROM python:3.11-slim
WORKDIR /app
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY semantic ./semantic
COPY dashboard ./dashboard
ENV AWS_REGION=us-east-1 ATHENA_WG=convergence-wg
EXPOSE 8080
CMD ["uvicorn", "dashboard.app:app", "--host", "0.0.0.0", "--port", "8080"]
