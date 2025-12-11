.PHONY: help start stop restart logs clean test dbt-run spark-job kafka-demo

help: ## Show this help message
	@echo "Data Engineering Platform - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

start: ## Start all services
	docker-compose up -d
	@echo "✓ Services started!"
	@echo "  Airflow: http://localhost:8080 (admin/admin)"
	@echo "  Spark: http://localhost:8081"
	@echo "  Jupyter: http://localhost:8888"

stop: ## Stop all services
	docker-compose down
	@echo "✓ Services stopped"

restart: ## Restart all services
	docker-compose restart
	@echo "✓ Services restarted"

logs: ## View logs (usage: make logs SERVICE=airflow-webserver)
	docker-compose logs -f $(SERVICE)

clean: ## Stop and remove all containers, volumes, and networks
	docker-compose down -v
	docker volume prune -f
	@echo "✓ Cleaned up all resources"

status: ## Check status of all services
	docker-compose ps

test: ## Run tests
	pytest tests/ -v

dbt-run: ## Run dbt models
	docker exec -it de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."

dbt-test: ## Run dbt tests
	docker exec -it de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

dbt-docs: ## Generate and serve dbt documentation
	docker exec -it de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir ."

spark-job: ## Run Spark aggregation job
	docker exec -it de_spark_master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark-jobs/data_aggregation.py

spark-transform: ## Run Spark transformation job
	docker exec -it de_spark_master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark-jobs/data_transformation.py

kafka-producer: ## Start Kafka producer (60 seconds)
	docker exec -it de_airflow_webserver python /opt/airflow/kafka/producer/event_producer.py --duration 60

kafka-consumer: ## Start Kafka consumer (60 seconds)
	docker exec -it de_airflow_webserver python /opt/airflow/kafka/consumer/event_consumer.py --duration 60

kafka-demo: ## Run Kafka demo (producer and consumer in parallel)
	@echo "Starting Kafka producer..."
	docker exec -d de_airflow_webserver python /opt/airflow/kafka/producer/event_producer.py --duration 60
	@sleep 2
	@echo "Starting Kafka consumer..."
	docker exec -it de_airflow_webserver python /opt/airflow/kafka/consumer/event_consumer.py --duration 60

airflow-shell: ## Open shell in Airflow container
	docker exec -it de_airflow_webserver bash

spark-shell: ## Open shell in Spark master container
	docker exec -it de_spark_master bash

postgres-shell: ## Open PostgreSQL shell
	docker exec -it de_postgres psql -U airflow -d airflow

init: ## Initialize environment (first time setup)
	cp .env.example .env || true
	@echo "✓ Environment initialized"
	@echo "  Edit .env file if needed, then run: make start"

rebuild: ## Rebuild and restart all services
	docker-compose down
	docker-compose build --no-cache
	docker-compose up -d
	@echo "✓ Services rebuilt and started"
