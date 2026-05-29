FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/proto/generated

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/common/laundry_common /app/laundry_common
COPY proto /app/proto
RUN mkdir -p /app/proto/generated && \
    python -m grpc_tools.protoc \
      -I /app/proto \
      --python_out=/app/proto/generated \
      --grpc_python_out=/app/proto/generated \
      /app/proto/*.proto

COPY services/identity-service/app /app/app

EXPOSE 8000 50051

CMD ["python", "-m", "app.main"]

