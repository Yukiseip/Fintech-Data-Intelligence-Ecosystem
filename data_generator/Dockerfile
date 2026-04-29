FROM python:3.11-slim

# Non-root user (security)
RUN groupadd -r fintech && useradd -r -g fintech fintech

# /pkg is the Python root; /pkg/data_generator is the package
WORKDIR /pkg

# Install dependencies first (layer caching).
# Context is ./data_generator, so '.' == the package contents.
COPY requirements.txt ./data_generator/
RUN pip install --no-cache-dir -r data_generator/requirements.txt

# Copy source into the data_generator package directory
COPY . ./data_generator/

# PYTHONPATH=/pkg  →  'from data_generator.models.x import X' resolves
ENV PYTHONPATH=/pkg
ENV PYTHONUNBUFFERED=1

USER fintech

# Entrypoint is the Python interpreter; CMD provides default args.
# `docker compose run --rm data-generator data_generator/main.py generate all`
# expands to: python data_generator/main.py generate all  (cwd=/pkg)
ENTRYPOINT ["python"]
CMD ["data_generator/main.py", "--help"]
