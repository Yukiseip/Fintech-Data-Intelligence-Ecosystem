.PHONY: up down test test-integration test-perf test-fraud test-dash logs clean infra generate lint help

# ══════════════════════════════════════════════════════════════════════
#  FINTECH DATA INTELLIGENCE ECOSYSTEM — Makefile
#  Single entrypoint for all operations.
#  Usage: make <target>
# ══════════════════════════════════════════════════════════════════════

## help: Show this help message
help:
	@echo "════════════════════════════════════════════════════════"
	@echo "  Fintech Analytics Engine — Available Commands"
	@echo "════════════════════════════════════════════════════════"
	@grep -E '^## ' Makefile | sed 's/## /  make /'

## up: Start the full stack (copies .env if missing, builds images, runs infra)
up:
	@echo "🚀 Starting Fintech Analytics Engine..."
	@if [ ! -f .env ]; then cp .env.example .env && echo "📄 Created .env from .env.example — update secrets before production!"; fi
	docker compose up -d --build
	@echo "⏳ Waiting for services to be healthy (60s)..."
	@sleep 60
	$(MAKE) infra
	@echo "✅ Stack is up! Services:"
	@echo "   Airflow:    http://localhost:8080  (admin/admin)"
	@echo "   Streamlit:  http://localhost:8501"
	@echo "   Spark UI:   http://localhost:8081"
	@echo "   Grafana:    http://localhost:3000  (admin/admin)"
	@echo "   Prometheus: http://localhost:9090"

## infra: Provision infrastructure (Terraform S3 in LocalStack)
infra:
	@echo "🏗️  Provisioning infrastructure via Terraform..."
	docker compose --profile infra run --rm terraform

## down: Stop and destroy everything (volumes included)
down:
	@echo "🛑 Stopping all services and removing volumes..."
	docker compose down -v --remove-orphans
	@echo "✅ Stack destroyed."

## generate: Generate synthetic transaction data and upload to S3
generate:
	@echo "🏭 Generating synthetic data..."
	docker compose run --rm data-generator data_generator/main.py generate all
	@echo "✅ Data generation complete."

## logs: Tail logs from all services
logs:
	docker compose logs -f --tail=100

## logs-airflow: Tail Airflow scheduler logs only
logs-airflow:
	docker compose logs -f --tail=100 airflow-scheduler

## logs-spark: Tail Spark master logs only
logs-spark:
	docker compose logs -f --tail=100 spark-master

## test: Run all unit tests (generator + processing + DAG integrity)
test:
	@echo "🧪 Running unit tests..."
	docker compose run --rm --no-deps data-generator pytest tests/ -v --tb=short
	docker compose exec airflow-scheduler python -m pytest /opt/airflow/dags/tests/ -v --tb=short
	@echo "✅ All unit tests passed."

## test-integration: Run integration tests (requires full stack)
test-integration:
	@echo "🔗 Running integration tests..."
	docker compose run --rm processing pytest tests/integration/ -v --tb=short -m integration
	@echo "✅ Integration tests complete."

## test-perf: Run throughput performance test (O1: 10K txns/min)
test-perf:
	@echo "⚡ Running performance test (10K txns/min target)..."
	docker compose run --rm processing pytest tests/performance/test_throughput.py -v --tb=short

## test-fraud: Run fraud detection benchmark (O2: >95% precision)
test-fraud:
	@echo "🚨 Running fraud detection benchmark..."
	docker compose run --rm processing pytest tests/performance/test_fraud_benchmark.py -v --tb=short

## test-dash: Run dashboard query latency test (O3: <2s p95)
test-dash:
	@echo "📊 Running dashboard query latency test..."
	docker compose run --rm analytics pytest tests/integration/test_dashboard_queries.py -v --tb=short

## dbt-run: Run dbt models
dbt-run:
	@echo "🔄 Running dbt models..."
	docker compose exec airflow-scheduler bash -c "cd /opt/dbt && dbt run --target dev"

## dbt-test: Run dbt tests (data quality gates)
dbt-test:
	@echo "🧪 Running dbt tests..."
	docker compose exec airflow-scheduler bash -c "cd /opt/dbt && dbt test --target dev"

## lint: Run flake8 + mypy on processing code
lint:
	@echo "🔍 Linting Python code..."
	docker compose run --rm processing flake8 jobs/ utils/ --max-line-length=100 --exclude=__pycache__
	docker compose run --rm processing mypy jobs/ utils/ --ignore-missing-imports

## clean: Remove all containers, images, volumes, and pycache
clean:
	@echo "🧹 Cleaning up everything..."
	docker compose down -v --remove-orphans --rmi local 2>/dev/null || true
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Clean complete."

## ps: Show status of all containers
ps:
	docker compose ps

## airflow-init: Manually re-initialize Airflow DB (if needed)
airflow-init:
	docker compose run --rm airflow-init
