FROM apache/airflow:2.9.0-python3.11

# Switch to root to install system dependencies if needed (optional)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         default-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user to install pip packages
USER airflow

# Install dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
