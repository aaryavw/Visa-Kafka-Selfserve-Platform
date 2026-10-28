# Visa-Kafka-Selfserve-Platform
Cloud-native microservice engine featuring self-service Apache Kafka cluster orchestration through containerized abstractions and sub-millisecond payment anomaly detection for fuel dispenser transactions and high-value digital currency settlements.

# Visa Kafka Cluster Self-Serve Provisioning & Low-Latency Transaction Monitoring Engine

## Overview
An enterprise-grade, high-throughput microservice built for Visa's Global Payment Systems team. This application enables engineering teams to self-serve Kafka clusters via containerized orchestration and provides real-time, low-latency transaction monitoring and anomaly detection for gas stations and digital currency routing.

## Features
- **Self-Serve Kafka Orchestration API:** Provision, inspect, and deprovision Kafka clusters dynamically.
- **Low-Latency Anomaly Detection Engine:** Real-time risk analysis for Automated Fuel Dispensers (MCC 5542) and high-value transactions.
- **Enterprise Observability:** Prometheus metrics endpoints (`/metrics`) exposing system throughput and detection counters.
- **Containerization & Cloud Native Ready:** Complete Dockerfile, Docker Compose, Kubernetes manifests, and GitHub Actions CI/CD workflow.

## Project Structure
```
visa_kafka_selfserve_platform/
├── src/
│   └── main.py
├── tests/
│   └── test_main.py
├── k8s/
│   └── deployment.yaml
├── .github/workflows/
│   └── ci.yml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

## Setup & Running Instructions

### Local Python Setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn src.main:app --reload --port 8000
```

### Running Tests
```bash
pytest -v
```

### Running with Docker Compose
```bash
docker-compose up --build -d
```

### Deploying to Kubernetes
```bash
kubectl apply -f k8s/deployment.yaml
