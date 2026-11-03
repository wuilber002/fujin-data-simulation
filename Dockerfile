FROM python:3.12-slim

ARG FUJIN_SERVICE_VERSION=0.1.0
ARG FUJIN_BUILD_REVISION=development
ENV FUJIN_SERVICE_VERSION=${FUJIN_SERVICE_VERSION} \
    FUJIN_BUILD_REVISION=${FUJIN_BUILD_REVISION}

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends openssl \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY scripts ./scripts
CMD ["uvicorn", "app.fujin_local:app", "--host", "0.0.0.0", "--port", "8080"]
