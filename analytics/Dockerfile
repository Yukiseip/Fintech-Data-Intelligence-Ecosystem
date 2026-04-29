FROM python:3.11-slim

# Non-root user
RUN groupadd -r fintech && useradd -r -g fintech -d /app fintech

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy analytics source
COPY . /app/analytics

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

USER fintech

EXPOSE 8501

CMD ["streamlit", "run", "analytics/app.py", \
     "--server.port=8501", \
     "--server.headless=true", \
     "--server.enableCORS=false"]
