FROM python:3.12.11-slim
LABEL org.opencontainers.image.authors="Hystax"

WORKDIR /src

COPY optscale_client/config_client optscale_client/config_client
COPY docker_images/aws_billing_reset/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docker_images/aws_billing_reset/aws-billing-reset.py .

CMD ["python", "/src/aws-billing-reset.py"]
