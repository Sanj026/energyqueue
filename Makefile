.PHONY: help run build down test benchmark workers jobs status logs clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

run: ## Start all services with Docker Compose
	docker-compose up

build: ## Build and start all services
	docker-compose up --build

down: ## Stop all services
	docker-compose down

test: ## Run all tests
	pytest tests/ -v --timeout=10

benchmark: ## Run the full benchmark
	./scripts/benchmark.sh

workers: ## Start the worker pool
	./scripts/start_workers.sh

jobs: ## Submit test jobs
	./scripts/submit_jobs.sh

status: ## Check queue status
	./scripts/check_status.sh

logs: ## Watch worker logs live
	tail -f logs/worker.log

clean: ## Remove pycache and logs
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -f logs/*.log logs/*.txt
