import pytest
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "HEALTHY", "system": "Visa Financial Infrastructure Gateway"}

def test_cluster_provision_and_lifecycle():
    # Provision cluster
    payload = {
        "cluster_id": "test-cluster-ashburn",
        "environment": "production",
        "brokers_count": 3,
        "retention_hours": 48,
        "partitions_default": 6
    }
    response = client.post("/api/v1/clusters", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["cluster_id"] == "test-cluster-ashburn"
    assert data["status"] == "RUNNING"
    assert "kafka-broker-test-cluster-ashburn" in data["connection_string"]

    # Duplicate creation error
    dup_response = client.post("/api/v1/clusters", json=payload)
    assert dup_response.status_code == 400

    # List clusters
    list_res = client.get("/api/v1/clusters")
    assert list_res.status_code == 200
    assert len(list_res.json()) >= 1

    # Deprovision cluster
    del_res = client.delete("/api/v1/clusters/test-cluster-ashburn")
    assert del_res.status_code == 200

def test_transaction_anomaly_detection_gas_station():
    # Gas station transaction over limit
    payload = {
        "transaction_id": "tx_gas_001",
        "amount": 185.50,
        "currency": "USD",
        "merchant_category_code": "5542",
        "account_id": "acc_user_99"
    }
    response = client.post("/api/v1/transactions/analyze", json=payload)
    assert response.status_code == 200
    res_data = response.json()
    assert res_data["is_flagged"] is True
    assert "Automated Fuel Dispenser" in res_data["reason"]
    assert res_data["processing_time_ms"] < 50.0  # Low latency threshold check

def test_transaction_anomaly_detection_normal():
    # Normal transaction
    payload = {
        "transaction_id": "tx_normal_002",
        "amount": 24.50,
        "currency": "USD",
        "merchant_category_code": "5812",
        "account_id": "acc_user_100"
    }
    response = client.post("/api/v1/transactions/analyze", json=payload)
    assert response.status_code == 200
    res_data = response.json()
    assert res_data["is_flagged"] is False
    assert res_data["reason"] is None
