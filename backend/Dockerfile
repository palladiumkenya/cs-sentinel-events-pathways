FROM python:3.11-slim
ENV TZ=Africa/Nairobi
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    curl \
    unixodbc \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 4100
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "4100"]
