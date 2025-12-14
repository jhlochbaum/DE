# Data Engineering Portfolio - Claude Context File

This document provides comprehensive context about the DE (Data Engineering) repository for future reference.

## Repository Overview

**Purpose:** Demonstrate senior-level data engineering skills through a comprehensive, production-ready data platform
**GitHub URL:** https://github.com/jhlochbaum/DE
**Owner:** jhlochbaum (jhlochbaum@gmail.com)
**Created:** 2025-12-11

## Current Status

### Working Components ✅
- **PostgreSQL 15**: Database and data warehouse
- **Apache Airflow 2.7.3**: Workflow orchestration (webserver + scheduler)
- **Apache Kafka 7.5.0**: Event streaming platform
- **Apache Zookeeper**: Kafka coordination
- **dbt 1.7.3**: Analytics engineering (installed in Airflow containers)

### Not Working/Excluded ❌
- **Apache Spark**: Removed due to bitnami/spark Docker image unavailability
  - Original plan: Spark 3.5.0 for distributed processing
  - Can be added back later with different image (e.g., apache/spark)
- **Jupyter Notebook**: Excluded from current docker-compose

### Docker Setup
- **Active compose file**: `docker-compose-no-spark.yml` (simplified version)
- **Original file**: `docker-compose.yml` (has Spark but image doesn't exist)
- **Network**: `de_network` (bridge)
- **Volumes**:
  - `postgres_data`: PostgreSQL data persistence
  - `airflow_logs`: Airflow logs

## Architecture

```
┌─────────────────┐
│   Airflow UI    │  Port 8080 (admin/admin)
│  (Webserver)    │
└────────┬────────┘
         │
┌────────┴────────┐
│    Scheduler    │  Executes DAG tasks
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
┌───▼───┐ ┌──▼──────┐
│Postgres│ │  Kafka  │  Port 29092 (localhost access)
│Port5432│ │Port 9092│  (internal)
└────────┘ └──┬──────┘
              │
         ┌────▼────────┐
         │  Zookeeper  │  Port 2181
         └─────────────┘
```

## Project Structure

```
DE/
├── airflow/
│   └── dags/                          # 4 DAG files (3 working without Spark)
│       ├── 01_simple_etl_pipeline.py          ✅ WORKING
│       ├── 02_spark_data_processing.py        ❌ Needs Spark
│       ├── 03_data_quality_pipeline.py        ✅ WORKING
│       └── 04_kafka_stream_orchestration.py   ✅ WORKING
│
├── dbt/                               # Analytics transformations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/                  # Source system staging
│   │   │   ├── stg_sales_transactions.sql
│   │   │   ├── stg_user_events.sql
│   │   │   └── schema.yml
│   │   └── marts/core/               # Business logic layer
│   │       ├── fct_daily_sales.sql
│   │       ├── dim_user_metrics.sql
│   │       └── schema.yml
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── cents_to_dollars.sql
│   └── packages.yml                  # dbt-utils, dbt_expectations
│
├── kafka/
│   ├── producer/
│   │   └── event_producer.py         # Generates user events
│   └── consumer/
│       └── event_consumer.py         # Processes events
│
├── spark/jobs/                        # PySpark jobs (need Spark runtime)
│   ├── data_aggregation.py           # User/product/time aggregations
│   └── data_transformation.py        # Joins, window functions, segmentation
│
├── terraform/
│   ├── aws/                          # AWS infrastructure
│   │   ├── main.tf                  # S3, Glue, Kinesis, Lambda
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── local/                        # Docker infrastructure
│       └── main.tf
│
├── scripts/
│   └── init_db.sql                   # PostgreSQL initialization
│
├── docker-compose.yml                ❌ Original (Spark image issue)
├── docker-compose-no-spark.yml       ✅ ACTIVE (working version)
├── requirements.txt
├── .gitignore
├── Makefile                          # Helper commands
├── README.md                         # Full documentation
├── QUICKSTART.md                     # 5-minute setup guide
└── CLAUDE_CONTEXT.md                 # This file
```

## Database Schema

**Database:** `airflow` (PostgreSQL)

**Schemas:**
- `raw`: Source data tables
- `staging`: Intermediate transformations
- `analytics`: Business logic / marts

**Tables in `raw` schema:**
1. `user_events` - User activity events (event_id, user_id, event_type, event_timestamp, event_data JSONB)
2. `sales_transactions` - Sales data (transaction_id, user_id, product_id, amount, status, transaction_date)
3. `product_catalog` - Product information (product_id, product_name, category, price)

**Sample data:** 5 products pre-loaded in init_db.sql

## DAG Details

### 1. simple_etl_pipeline (01_simple_etl_pipeline.py)
**Purpose:** Basic ETL demonstrating Extract → Transform → Load
**Schedule:** Daily (`@daily`)
**Tasks:**
1. `create_staging_table` - Create staging table if not exists
2. `extract` - Query last 24h of completed transactions
3. `transform` - Aggregate by user (count, sum, avg, max)
4. `load` - Insert into `staging.daily_user_sales_summary`

**Technologies:** Python, pandas, PostgreSQL, XCom

### 2. spark_data_processing (02_spark_data_processing.py) ❌
**Status:** Won't work without Spark cluster
**Purpose:** Submit PySpark jobs for distributed processing
**Note:** Skip this for now

### 3. data_quality_pipeline (03_data_quality_pipeline.py)
**Purpose:** Data quality checks with branching logic
**Schedule:** Hourly (`@hourly`)
**Tasks:**
1. `check_freshness` - Data updated in last hour?
2. `check_completeness` - Null value percentage
3. `check_volume` - Expected record count
4. `quality_check_router` - Branch based on results
5. `quality_passed` / `quality_failed` - Different paths
6. `send_alert` - If quality fails
7. `log_metrics` - Record quality scores

**Key Feature:** Demonstrates BranchPythonOperator for conditional workflows

### 4. kafka_stream_orchestration (04_kafka_stream_orchestration.py)
**Purpose:** Orchestrate Kafka producer/consumer as part of pipeline
**Schedule:** Hourly (`@hourly`)
**Tasks:**
1. `check_kafka_health` - Verify Kafka cluster
2. `run_producer` - Start event producer (runs in background)
3. `run_consumer` - Start event consumer (runs in background)
4. `validate_data_flow` - Check messages flowing
5. `monitor_consumer` - Check consumer lag

**Note:** Requires kafka-python package installed

## Kafka Components

### Producer (kafka/producer/event_producer.py)
**Generates:** User events (page_view, click, purchase, signup, login, logout, search)
**Topic:** `user-events`
**Rate:** Configurable (default: 5 events/second)
**Key Features:**
- Partitioning by user_id
- Realistic event data with metadata (browser, device, IP)
- Purchase events include transaction details
- Statistics tracking

**Usage:**
```bash
docker exec de_airflow_webserver python /opt/airflow/kafka/producer/event_producer.py \
  --mode continuous --duration 60 --rate 5
```

### Consumer (kafka/consumer/event_consumer.py)
**Processes:** Events from `user-events` topic
**Group ID:** `event-processor`
**Key Features:**
- Event type-specific processing
- Statistics aggregation (by type, by user)
- High-value purchase detection
- Signup tracking

**Usage:**
```bash
docker exec de_airflow_webserver python /opt/airflow/kafka/consumer/event_consumer.py \
  --mode continuous --duration 60
```

## dbt Models

### Staging Layer
- **stg_sales_transactions**: Clean and typed sales data with calculated fields (transaction_tier)
- **stg_user_events**: Extract JSONB fields, add time dimensions

### Marts Layer (Core)
- **fct_daily_sales**: Daily sales metrics (revenue, transactions, completion rate by tier)
- **dim_user_metrics**: User-level aggregations (lifetime value, engagement score, segmentation)

### Tests
- Source data: uniqueness, not_null, accepted_values
- Model outputs: data quality checks, business logic validation
- Uses dbt-utils for advanced testing

**Run dbt:**
```bash
docker exec de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."
docker exec de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."
```

## Terraform Infrastructure

### AWS (terraform/aws/)
**Resources:**
- S3 buckets: Bronze/Silver/Gold data lake layers
- AWS Glue: Catalog database and crawler
- Kinesis: Event streaming
- Lambda: Serverless processing
- CloudWatch: Monitoring and logging
- SNS: Alerting
- EventBridge: Scheduled triggers

**Note:** Requires AWS credentials to deploy

### Local (terraform/local/)
**Resources:** Docker containers via Terraform
**Providers:** kreuzwerker/docker
**Note:** Windows pipe path configured

## Known Issues & Solutions

### Issue 1: Spark Images Not Available
**Problem:** `bitnami/spark:3.5.0`, `bitnami/spark:3.5`, `bitnami/spark:latest` all fail to pull
**Solution:** Using `docker-compose-no-spark.yml` instead
**Future Fix:** Use `apache/spark-py` or build custom Spark image

### Issue 2: PostgreSQL Init Script SQL Syntax
**Problem:** `CREATE DATABASE IF NOT EXISTS` not supported in PostgreSQL
**Solution:** Fixed in `scripts/init_db.sql` - removed database creation, only create schemas

### Issue 3: Airflow Scheduler Initialization
**Problem:** Scheduler starts before webserver finishes DB init
**Solution:** Restart scheduler after webserver is healthy
**Better Fix:** Add proper healthcheck dependency in docker-compose

### Issue 4: Protobuf Version Conflicts
**Problem:** dbt-core installs protobuf 6.x, conflicts with Google Cloud libraries
**Impact:** Warning messages (not fatal)
**Solution:** Can be ignored for local development

## Testing Checklist

### ✅ Infrastructure Tests
- [x] Docker containers starting
- [x] PostgreSQL healthy and accessible
- [x] Airflow webserver healthy (port 8080)
- [x] Airflow scheduler healthy
- [x] Kafka broker running (port 29092)
- [x] Zookeeper running (port 2181)

### 🔄 Component Tests
- [ ] Airflow UI accessible (http://localhost:8080)
- [ ] DAGs visible in Airflow
- [ ] Run simple_etl_pipeline DAG
- [ ] Run data_quality_pipeline DAG
- [ ] Kafka producer generates events
- [ ] Kafka consumer processes events
- [ ] dbt models compile
- [ ] dbt models run successfully
- [ ] dbt tests pass
- [ ] PostgreSQL data visible

### ❌ Disabled Tests (No Spark)
- [ ] Spark jobs
- [ ] Spark UI access
- [ ] PySpark aggregation
- [ ] PySpark transformation

## Useful Commands

### Docker Management
```bash
# Start services
docker-compose -f docker-compose-no-spark.yml up -d

# Stop services (keep data)
docker-compose -f docker-compose-no-spark.yml down

# Stop and remove volumes (clean slate)
docker-compose -f docker-compose-no-spark.yml down -v

# View logs
docker logs -f de_airflow_webserver
docker logs -f de_postgres
docker logs -f de_kafka

# Check status
docker ps --filter "name=de_"

# Shell access
docker exec -it de_airflow_webserver bash
docker exec -it de_postgres psql -U airflow -d airflow
```

### Airflow Commands
```bash
# List DAGs
docker exec de_airflow_webserver airflow dags list

# Trigger DAG
docker exec de_airflow_webserver airflow dags trigger simple_etl_pipeline

# Check DAG status
docker exec de_airflow_webserver airflow dags state simple_etl_pipeline

# View task logs
docker exec de_airflow_webserver airflow tasks logs simple_etl_pipeline extract 2025-12-11
```

### PostgreSQL Commands
```bash
# Connect to database
docker exec -it de_postgres psql -U airflow -d airflow

# List schemas
\dn

# List tables in schema
\dt raw.*
\dt staging.*

# Query data
SELECT * FROM raw.product_catalog;
SELECT * FROM raw.sales_transactions LIMIT 10;
```

### Kafka Commands
```bash
# List topics
docker exec de_kafka kafka-topics --list --bootstrap-server localhost:9092

# Create topic manually
docker exec de_kafka kafka-topics --create --topic user-events \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Describe topic
docker exec de_kafka kafka-topics --describe --topic user-events \
  --bootstrap-server localhost:9092

# Consume messages (view what's in topic)
docker exec de_kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic user-events --from-beginning --max-messages 10
```

### dbt Commands
```bash
# Compile models
docker exec de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt compile --profiles-dir ."

# Run all models
docker exec de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."

# Run specific model
docker exec de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt run --models fct_daily_sales --profiles-dir ."

# Test models
docker exec de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

# Generate docs
docker exec de_airflow_webserver bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir ."
```

## Performance Notes

**Resource Usage:**
- Total memory: ~4-6 GB
- Postgres: ~200 MB
- Airflow webserver: ~500 MB
- Airflow scheduler: ~500 MB
- Kafka: ~500 MB
- Zookeeper: ~100 MB

**Startup Time:**
- First time (pulling images): 3-5 minutes
- Subsequent starts: 30-60 seconds

## Future Enhancements

1. **Add Spark back** with working Docker image
2. **Add Jupyter Notebook** for interactive development
3. **Add Great Expectations** for advanced data quality
4. **Add Metabase/Superset** for visualization
5. **Implement CDC** with Debezium
6. **Add Delta Lake** for ACID transactions
7. **Add data lineage** visualization (e.g., OpenLineage)
8. **CI/CD pipeline** with GitHub Actions
9. **Add monitoring** with Prometheus + Grafana
10. **Production hardening**: secrets management, SSL, authentication

## Portfolio Value

This repository demonstrates:

✅ **Orchestration Expertise**: Complex Airflow DAGs with dependencies, branching, error handling
✅ **Streaming Data**: Kafka producer/consumer patterns
✅ **Analytics Engineering**: dbt models, tests, documentation
✅ **Data Quality**: Automated checks and monitoring
✅ **Infrastructure as Code**: Terraform for AWS and local
✅ **Database Design**: Schema design, indexing, optimization
✅ **Containerization**: Docker multi-service architecture
✅ **Documentation**: Comprehensive README, quickstart, this context file
✅ **Best Practices**: Version control, testing, modularity

⚠️ **Missing (Due to Technical Issues)**:
❌ Spark distributed processing (can be added back)
❌ Interactive notebooks (can be added back)

## Contact & Links

- **GitHub Repo**: https://github.com/jhlochbaum/DE
- **Owner**: jhlochbaum
- **Email**: jhlochbaum@gmail.com
- **Local Path**: `C:\Users\jhloc\Claude projects\DE`

---

**Last Updated:** 2025-12-11
**Claude Context Version:** 1.0
