terraform {
  required_version = ">= 1.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {
  host = "npipe:////./pipe/docker_engine"  # Windows
  # host = "unix:///var/run/docker.sock"  # Linux/Mac
}

# Network
resource "docker_network" "de_network" {
  name = "de_network"
}

# PostgreSQL
resource "docker_image" "postgres" {
  name = "postgres:15"
}

resource "docker_container" "postgres" {
  name  = "de_postgres"
  image = docker_image.postgres.image_id

  env = [
    "POSTGRES_USER=airflow",
    "POSTGRES_PASSWORD=airflow",
    "POSTGRES_DB=airflow"
  ]

  ports {
    internal = 5432
    external = 5432
  }

  volumes {
    volume_name    = docker_volume.postgres_data.name
    container_path = "/var/lib/postgresql/data"
  }

  networks_advanced {
    name = docker_network.de_network.name
  }

  healthcheck {
    test     = ["CMD", "pg_isready", "-U", "airflow"]
    interval = "5s"
    timeout  = "3s"
    retries  = 5
  }
}

resource "docker_volume" "postgres_data" {
  name = "de_postgres_data"
}

# Kafka (Zookeeper)
resource "docker_image" "zookeeper" {
  name = "confluentinc/cp-zookeeper:7.5.0"
}

resource "docker_container" "zookeeper" {
  name  = "de_zookeeper"
  image = docker_image.zookeeper.image_id

  env = [
    "ZOOKEEPER_CLIENT_PORT=2181",
    "ZOOKEEPER_TICK_TIME=2000"
  ]

  ports {
    internal = 2181
    external = 2181
  }

  networks_advanced {
    name = docker_network.de_network.name
  }
}

# Kafka Broker
resource "docker_image" "kafka" {
  name = "confluentinc/cp-kafka:7.5.0"
}

resource "docker_container" "kafka" {
  name  = "de_kafka"
  image = docker_image.kafka.image_id

  depends_on = [docker_container.zookeeper]

  env = [
    "KAFKA_BROKER_ID=1",
    "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
    "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1"
  ]

  ports {
    internal = 9092
    external = 9092
  }

  ports {
    internal = 29092
    external = 29092
  }

  networks_advanced {
    name = docker_network.de_network.name
  }
}

# Outputs
output "postgres_connection_string" {
  value     = "postgresql://airflow:airflow@localhost:5432/airflow"
  sensitive = true
}

output "kafka_bootstrap_servers" {
  value = "localhost:29092"
}

output "network_name" {
  value = docker_network.de_network.name
}
