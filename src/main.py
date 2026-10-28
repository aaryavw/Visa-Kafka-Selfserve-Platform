from fastapi import FastAPI, HTTPException, status
from fastapi.responses import Response
from pydantic import BaseModel, Field
from typing import Dict, Optional, List
import time
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = FastAPI(
    title="Visa Kafka Provisioning & Transaction Monitoring Engine",
    description="Self-serve Kafka cluster orchestration and real-time low-latency payment anomaly detection platform.",
    version="1.0.0"
)

# Prometheus Metrics
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP Requests', ['method', 'endpoint', 'status_code'])
ANOMALY_COUNT = Counter('payment_anomalies_detected_total', 'Total payment transaction anomalies detected', ['reason'])
PROCESSING_LATENCY = Histogram('transaction_processing_seconds', 'Latency of transaction anomaly check in seconds')

# In-memory storage for Kafka clusters
clusters_db: Dict[str, dict] = {}

class ClusterCreateRequest(BaseModel):
    cluster_id: str = Field(..., example="visa-settlement-cluster-01")
    environment: str = Field("development", example="production")
    brokers_count: int = Field(3, ge=1, le=10)
    retention_hours: int = Field(72, ge=1)
    partitions_default: int = Field(12, ge=1)

class ClusterResponse(BaseModel):
    cluster_id: str
    status: str
    brokers_count: int
    connection_string: str
    created_at: float

class Transaction(BaseModel):
    transaction_id: str = Field(..., example="tx_visa_984123")
    amount: float = Field(..., gt=0, example=45000.0)
    currency: str = Field("USD", example="USD")
    merchant_category_code: str = Field(..., example="5542")  # Automated Fuel Dispensers
    timestamp: float = Field(default_factory=time.time)
    account_id: str = Field(..., example="acc_881923")

class AnomalyResult(BaseModel):
    transaction_id: str
    is_flagged: bool
    risk_score: float
    reason: Optional[str] = None
    processing_time_ms: float

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    return {"status": "HEALTHY", "system": "Visa Financial Infrastructure Gateway"}

@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/api/v1/clusters", response_model=ClusterResponse, status_code=status.HTTP_201_CREATED)
def provision_cluster(request: ClusterCreateRequest):
    REQUEST_COUNT.labels(method="POST", endpoint="/api/v1/clusters", status_code=201).inc()
    if request.cluster_id in clusters_db:
        raise HTTPException(status_code=400, detail=f"Cluster '{request.cluster_id}' already exists.")
    
    cluster_info = {
        "cluster_id": request.cluster_id,
        "status": "RUNNING",
        "brokers_count": request.brokers_count,
        "connection_string": f"kafka-broker-{request.cluster_id}.visa.internal:9092",
        "created_at": time.time()
    }
    clusters_db[request.cluster_id] = cluster_info
    return cluster_info

@app.get("/api/v1/clusters", response_model=List[ClusterResponse])
def list_clusters():
    REQUEST_COUNT.labels(method="GET", endpoint="/api/v1/clusters", status_code=200).inc()
    return list(clusters_db.values())

@app.delete("/api/v1/clusters/{cluster_id}", status_code=status.HTTP_200_OK)
def deprovision_cluster(cluster_id: str):
    if cluster_id not in clusters_db:
        REQUEST_COUNT.labels(method="DELETE", endpoint=f"/api/v1/clusters/{cluster_id}", status_code=404).inc()
        raise HTTPException(status_code=404, detail="Cluster not found.")
    del clusters_db[cluster_id]
    REQUEST_COUNT.labels(method="DELETE", endpoint=f"/api/v1/clusters/{cluster_id}", status_code=200).inc()
    return {"message": f"Cluster '{cluster_id}' successfully deprovisioned."}

@app.post("/api/v1/transactions/analyze", response_model=AnomalyResult)
def analyze_transaction(tx: Transaction):
    start_time = time.perf_counter()
    is_flagged = False
    risk_score = 0.12
    reason = None

    # Low-latency rule engine logic (gas station / high-value automated checks)
    if tx.merchant_category_code == "5542" and tx.amount > 150.0:
        is_flagged = True
        risk_score = 0.95
        reason = "Automated Fuel Dispenser transaction exceeds single-swipe safety ceiling ($150)."
        ANOMALY_COUNT.labels(reason="gas_station_over_limit").inc()
    elif tx.amount > 10000.0:
        is_flagged = True
        risk_score = 0.88
        reason = "High-value velocity alert: Transaction amount exceeds $10,000 threshold."
        ANOMALY_COUNT.labels(reason="high_value_transaction").inc()

    latency_ms = (time.perf_counter() - start_time) * 1000.0
    PROCESSING_LATENCY.observe(latency_ms / 1000.0)
    REQUEST_COUNT.labels(method="POST", endpoint="/api/v1/transactions/analyze", status_code=200).inc()

    return AnomalyResult(
        transaction_id=tx.transaction_id,
        is_flagged=is_flagged,
        risk_score=risk_score,
        reason=reason,
        processing_time_ms=round(latency_ms, 3)
    )
