FROM debian:bookworm-slim
LABEL org.opencontainers.image.authors="Tessell Finops"

RUN apt-get update \
    && apt-get install -y --no-install-recommends mariadb-client \
    && rm -rf /var/lib/apt/lists/*

COPY docker_images/aws_billing_reset/aws-billing-reset.sh /usr/local/bin/aws-billing-reset.sh
RUN chmod +x /usr/local/bin/aws-billing-reset.sh

CMD ["/usr/local/bin/aws-billing-reset.sh"]
