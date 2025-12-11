# Data Engineering Portfolio

A comprehensive data engineering portfolio demonstrating modern data stack technologies, best practices, and real-world patterns for building scalable data platforms.

## 🎯 Overview

This repository showcases end-to-end data engineering capabilities including:

- **Orchestration**: Apache Airflow for workflow management
- **Distributed Processing**: Apache Spark for large-scale data transformations
- **Stream Processing**: Kafka for real-time event streaming
- **Analytics Engineering**: dbt for transformation and testing
- **Infrastructure as Code**: Terraform for AWS and local infrastructure
- **Data Quality**: Testing and validation patterns
- **Documentation**: Comprehensive documentation and lineage

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                              │
│              APIs • Databases • Event Streams                    │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Ingestion Layer                               │
│        Kafka Producers • Airflow DAGs • Stream Processors        │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Storage Layer (Data Lake)                      │
│           S3/Local Storage • PostgreSQL • Delta Lake            │
│             Bronze (Raw) → Silver (Processed) → Gold             │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Processing Layer                                 │
│         Apache Spark • dbt • Data Quality Checks                │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Analytics Layer                                  │
│          Data Marts • Metrics • Aggregations                    │
└─────────────────────────────────────────────────────────────────┘
```

## 📦 Tech Stack

### Core Technologies
- **Python 3.11+**: Primary programming language
- **Apache Airflow 2.7.3**: Workflow orchestration
- **Apache Spark 3.5.0**: Distributed data processing
- **Apache Kafka 7.5.0**: Event streaming platform
- **PostgreSQL 15**: Relational database and data warehouse
- **dbt 1.7.3**: Analytics engineering and transformations

### Infrastructure
- **Docker & Docker Compose**: Containerization
- **Terraform**: Infrastructure as Code
- **AWS Services**: S3, Glue, Kinesis, Lambda, CloudWatch

### Development
- **Jupyter**: Interactive development
- **Great Expectations**: Data quality validation
- **pytest**: Testing framework

## 🚀 Quick Start

### Prerequisites

- Docker Desktop installed and running
- Python 3.11+
- Git

### 1. Clone Repository

```bash
git clone https://github.com/yourusername/DE.git
cd DE
```

### 2. Set Up Environment

```bash
# Copy environment template
cp .env.example .env

# Install Python dependencies (optional for local development)
pip install -r requirements.txt
```

### 3. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 4. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow Web UI | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8081 | - |
| Jupyter Notebook | http://localhost:8888 | Check logs for token |
| PostgreSQL | localhost:5432 | airflow / airflow |
| Kafka | localhost:29092 | - |

## 📂 Project Structure

```
DE/
├── airflow/
│   ├── dags/                      # Airflow DAG definitions
│   │   ├── 01_simple_etl_pipeline.py
│   │   ├── 02_spark_data_processing.py
│   │   ├── 03_data_quality_pipeline.py
│   │   └── 04_kafka_stream_orchestration.py
│   ├── plugins/                   # Custom Airflow plugins
│   └── config/                    # Airflow configurations
│
├── spark/
│   ├── jobs/                      # PySpark job scripts
│   │   ├── data_aggregation.py
│   │   └── data_transformation.py
│   └── data/                      # Sample data files
│
├── kafka/
│   ├── producer/                  # Kafka producers
│   │   └── event_producer.py
│   └── consumer/                  # Kafka consumers
│       └── event_consumer.py
│
├── dbt/
│   ├── models/
│   │   ├── staging/              # Staging models
│   │   │   ├── stg_sales_transactions.sql
│   │   │   ├── stg_user_events.sql
│   │   │   └── schema.yml
│   │   └── marts/                # Data marts
│   │       └── core/
│   │           ├── fct_daily_sales.sql
│   │           ├── dim_user_metrics.sql
│   │           └── schema.yml
│   ├── macros/                   # Custom dbt macros
│   ├── tests/                    # Data tests
│   └── dbt_project.yml
│
├── terraform/
│   ├── aws/                      # AWS infrastructure
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── local/                    # Local Docker infrastructure
│       └── main.tf
│
├── scripts/                      # Utility scripts
│   └── init_db.sql
│
├── notebooks/                    # Jupyter notebooks
├── data/                         # Data directories
│   ├── raw/
│   ├── processed/
│   └── output/
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 💡 Key Features & Examples

### 1. Airflow DAGs

#### Simple ETL Pipeline
Demonstrates basic Extract, Transform, Load pattern:
- Extracts data from PostgreSQL
- Applies transformations using pandas
- Loads results to staging tables

```bash
# Trigger DAG manually
docker exec -it de_airflow_scheduler airflow dags trigger simple_etl_pipeline
```

#### Data Quality Pipeline
Shows data quality checks with branching logic:
- Freshness checks
- Completeness validation
- Volume monitoring
- Alert generation

#### Spark Integration
Orchestrates Spark jobs for large-scale processing:
- Submits PySpark jobs to Spark cluster
- Manages job parameters
- Validates output

### 2. PySpark Jobs

#### Data Aggregation
```bash
docker exec -it de_spark_master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/data_aggregation.py \
  --output /opt/spark-data/processed
```

Features:
- User-level aggregations
- Product-level metrics
- Time-based analysis
- Partitioned output

#### Data Transformation
Complex transformations with:
- Multi-table joins
- Window functions
- User segmentation
- Advanced analytics

### 3. Kafka Streaming

#### Start Producer
```bash
docker exec -it de_kafka python /app/kafka/producer/event_producer.py \
  --mode continuous \
  --duration 300 \
  --rate 5
```

#### Start Consumer
```bash
docker exec -it de_kafka python /app/kafka/consumer/event_consumer.py \
  --mode continuous \
  --duration 300
```

### 4. dbt Models

#### Run dbt Models
```bash
# Inside Airflow container or locally
dbt run --profiles-dir ./dbt --project-dir ./dbt

# Run specific model
dbt run --models fct_daily_sales --profiles-dir ./dbt --project-dir ./dbt

# Run tests
dbt test --profiles-dir ./dbt --project-dir ./dbt

# Generate documentation
dbt docs generate --profiles-dir ./dbt --project-dir ./dbt
dbt docs serve --profiles-dir ./dbt --project-dir ./dbt
```

### 5. Terraform Infrastructure

#### Local Infrastructure
```bash
cd terraform/local
terraform init
terraform plan
terraform apply
```

#### AWS Infrastructure
```bash
cd terraform/aws

# Initialize
terraform init

# Plan (requires AWS credentials)
terraform plan -var="environment=dev" -var="project_name=de-platform"

# Apply (requires AWS credentials)
terraform apply -var="environment=dev" -var="project_name=de-platform"
```

## 🧪 Testing

### Run Python Tests
```bash
pytest tests/
```

### Run dbt Tests
```bash
dbt test --profiles-dir ./dbt --project-dir ./dbt
```

### Data Quality Checks
Great Expectations integration for automated data validation.

## 📊 Monitoring & Observability

### Airflow Monitoring
- DAG run history
- Task duration metrics
- Failure alerts
- SLA monitoring

### Data Quality Metrics
- Freshness checks
- Completeness scores
- Volume anomalies
- Schema validation

### Infrastructure Monitoring
- Resource utilization
- Container health checks
- Log aggregation

## 🎓 Learning Resources

### Concepts Demonstrated

1. **Medallion Architecture** (Bronze/Silver/Gold)
2. **Slowly Changing Dimensions** (SCD Type 2)
3. **Incremental Processing**
4. **Idempotency in Data Pipelines**
5. **Data Quality Frameworks**
6. **Stream vs Batch Processing**
7. **Data Lineage & Documentation**
8. **Infrastructure as Code**

### Best Practices

- ✅ Version control for all code
- ✅ Comprehensive testing (unit, integration, data quality)
- ✅ Clear documentation and lineage
- ✅ Monitoring and alerting
- ✅ Modular and reusable components
- ✅ Error handling and retry logic
- ✅ Cost optimization strategies
- ✅ Security best practices

## 🛠️ Troubleshooting

### Services Not Starting

```bash
# Check logs
docker-compose logs -f [service_name]

# Restart services
docker-compose restart

# Rebuild containers
docker-compose up -d --build
```

### Airflow Webserver Issues

```bash
# Reset Airflow database
docker exec -it de_airflow_webserver airflow db reset

# Create admin user
docker exec -it de_airflow_webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### Database Connection Issues

```bash
# Test PostgreSQL connection
docker exec -it de_postgres psql -U airflow -d airflow

# Check database status
docker exec -it de_postgres pg_isready
```

## 📈 Performance Optimization

### Spark Tuning
- Partition sizing
- Memory configuration
- Shuffle optimization
- Broadcast joins

### Airflow Optimization
- Parallel task execution
- Connection pooling
- XCom size management
- Task concurrency limits

### Database Optimization
- Indexing strategies
- Query optimization
- Vacuum and analyze
- Partitioning

## 🔐 Security Considerations

- Secrets management (use environment variables or secret managers)
- Network isolation
- Role-based access control (RBAC)
- Data encryption at rest and in transit
- Audit logging

## 🚧 Future Enhancements

- [ ] Add Metabase/Superset for visualization
- [ ] Implement Great Expectations for data quality
- [ ] Add Dagster as alternative orchestrator
- [ ] Implement CDC with Debezium
- [ ] Add Delta Lake for ACID transactions
- [ ] Integrate with cloud data warehouses (Snowflake, BigQuery)
- [ ] Add data lineage visualization
- [ ] Implement feature store

## 📝 License

MIT License - feel free to use this for learning and portfolio purposes.

## 🤝 Contributing

Contributions welcome! Please feel free to submit a Pull Request.

## 📧 Contact

For questions or collaboration opportunities, please reach out via [your contact method].

---

**Note**: This is a demonstration project for portfolio purposes. For production use, additional security, monitoring, and optimization would be required.
