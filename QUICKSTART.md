# Quick Start Guide

Get up and running with the Data Engineering platform in 5 minutes!

## ⚡ 5-Minute Setup

### Step 1: Start Services (1 minute)

```bash
# Navigate to project directory
cd DE

# Start all services
docker-compose up -d

# Wait for services to be healthy (30 seconds)
docker-compose ps
```

### Step 2: Verify Services (1 minute)

```bash
# Check Airflow
curl http://localhost:8080/health

# Check Spark
curl http://localhost:8081

# Check Postgres
docker exec -it de_postgres pg_isready
```

### Step 3: Access Airflow (30 seconds)

1. Open browser: http://localhost:8080
2. Login with: `admin` / `admin`
3. You should see 4 DAGs ready to run

### Step 4: Run First DAG (2 minutes)

1. In Airflow UI, find **simple_etl_pipeline**
2. Click the **Play** button (▶️) on the right
3. Click **Trigger DAG**
4. Watch the DAG run in real-time!

### Step 5: Explore (30 seconds)

Check out the other services:

- **Spark UI**: http://localhost:8081
- **Jupyter**: http://localhost:8888 (check logs for token)
- **Postgres**: `psql -h localhost -U airflow -d airflow`

## 🎯 What to Try Next

### Run a Spark Job

```bash
docker exec -it de_spark_master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/data_aggregation.py
```

### Start Kafka Streaming

Terminal 1 (Producer):
```bash
docker exec -it de_airflow_webserver python /opt/airflow/kafka/producer/event_producer.py --duration 60
```

Terminal 2 (Consumer):
```bash
docker exec -it de_airflow_webserver python /opt/airflow/kafka/consumer/event_consumer.py --duration 60
```

### Run dbt Models

```bash
docker exec -it de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."
```

## 🐛 Quick Troubleshooting

**Services won't start?**
```bash
docker-compose down -v
docker-compose up -d
```

**Airflow webserver not accessible?**
```bash
docker-compose restart airflow-webserver
docker-compose logs -f airflow-webserver
```

**Need to reset everything?**
```bash
docker-compose down -v
docker volume prune -f
docker-compose up -d
```

## 📚 Learn More

- Read the full [README.md](README.md) for detailed documentation
- Check out the [architecture diagram](README.md#-architecture)
- Explore individual components in their respective directories

## 💡 Pro Tips

1. **Monitor resources**: Use `docker stats` to watch resource usage
2. **View logs**: `docker-compose logs -f [service_name]`
3. **Shell access**: `docker exec -it [container_name] bash`
4. **Stop services**: `docker-compose down` (keeps data)
5. **Clean slate**: `docker-compose down -v` (removes data)

Happy Data Engineering! 🚀
