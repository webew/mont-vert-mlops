FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    mlflow \
    scikit-learn \
    boto3 \
    python-dotenv \
    pydantic

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
