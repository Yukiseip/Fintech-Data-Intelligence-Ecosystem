FROM bitnami/spark:3.5 AS base

USER root

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy processing source
WORKDIR /app
COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Non-root user (bitnami spark uses 1001)
USER 1001

ENTRYPOINT ["python"]
CMD ["-m", "pytest", "tests/", "-v"]
