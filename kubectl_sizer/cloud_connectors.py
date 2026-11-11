"""Cloud & Kubernetes API connectors for live AKS, EKS, and GKE workload telemetry."""

from __future__ import annotations

import json
import subprocess
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class LiveWorkloadTelemetry:
    """Represents live resource allocation and observed traffic from AWS EKS, Azure AKS, or GCP GKE."""

    cloud_provider: str          # "aws", "azure", "gcp", or "k8s"
    cluster_name: str
    namespace: str
    service: str
    runtime: str
    current_replicas: int
    current_max_replicas: int
    current_cpu_req_m: int
    current_cpu_lim_m: int
    current_mem_req_mi: int
    current_mem_lim_mi: int
    observed_avg_tps: int
    observed_peak_tps: int
    observed_p95_latency_ms: int
    observed_cpu_ms_per_req: float
    observed_avg_cpu_usage_m: int
    observed_peak_mem_usage_mi: int
    has_db: bool = True


# Built-in realistic cloud cluster telemetry snapshots for `--simulate-cloud` demos & tests
DEMO_CLOUD_CLUSTERS: Dict[str, List[LiveWorkloadTelemetry]] = {
    "azure": [
        LiveWorkloadTelemetry(
            cloud_provider="azure",
            cluster_name="aks-prod-canadacentral-01",
            namespace="prod-banking",
            service="payment-core-api",
            runtime="jvm",
            current_replicas=2,
            current_max_replicas=4,
            current_cpu_req_m=250,
            current_cpu_lim_m=500,
            current_mem_req_mi=512,
            current_mem_lim_mi=512,
            observed_avg_tps=620,
            observed_peak_tps=1860,
            observed_p95_latency_ms=55,
            observed_cpu_ms_per_req=4.2,
            observed_avg_cpu_usage_m=460,  # Throttling! Using 460m on 250m req / 500m lim
            observed_peak_mem_usage_mi=490,
            has_db=True,
        ),
        LiveWorkloadTelemetry(
            cloud_provider="azure",
            cluster_name="aks-prod-canadacentral-01",
            namespace="prod-banking",
            service="statement-pdf-worker",
            runtime="jvm",
            current_replicas=10,
            current_max_replicas=16,
            current_cpu_req_m=2000,
            current_cpu_lim_m=4000,
            current_mem_req_mi=2048,
            current_mem_lim_mi=2048,
            observed_avg_tps=90,
            observed_peak_tps=180,
            observed_p95_latency_ms=35,
            observed_cpu_ms_per_req=3.0,
            observed_avg_cpu_usage_m=140,  # Heavily over-provisioned (7% CPU utilization)
            observed_peak_mem_usage_mi=620,
            has_db=True,
        ),
    ],
    "aws": [
        LiveWorkloadTelemetry(
            cloud_provider="aws",
            cluster_name="eks-prod-us-east-1",
            namespace="prod-core",
            service="checkout-orchestrator",
            runtime="go",
            current_replicas=8,
            current_max_replicas=14,
            current_cpu_req_m=1000,
            current_cpu_lim_m=2000,
            current_mem_req_mi=1024,
            current_mem_lim_mi=1024,
            observed_avg_tps=350,
            observed_peak_tps=700,
            observed_p95_latency_ms=18,
            observed_cpu_ms_per_req=1.2,
            observed_avg_cpu_usage_m=110,  # Over-provisioned on EKS
            observed_peak_mem_usage_mi=210,
            has_db=False,
        ),
        LiveWorkloadTelemetry(
            cloud_provider="aws",
            cluster_name="eks-prod-us-east-1",
            namespace="prod-core",
            service="fraud-scoring-svc",
            runtime="python",
            current_replicas=2,
            current_max_replicas=4,
            current_cpu_req_m=500,
            current_cpu_lim_m=1000,
            current_mem_req_mi=512,
            current_mem_lim_mi=512,
            observed_avg_tps=420,
            observed_peak_tps=1680,
            observed_p95_latency_ms=85,
            observed_cpu_ms_per_req=8.5,
            observed_avg_cpu_usage_m=920,  # Under-provisioned & saturating CPU
            observed_peak_mem_usage_mi=485,
            has_db=True,
        ),
    ],
    "gcp": [
        LiveWorkloadTelemetry(
            cloud_provider="gcp",
            cluster_name="gke-prod-asia-south1",
            namespace="prod-streaming",
            service="kafka-event-ingestor",
            runtime="jvm",
            current_replicas=3,
            current_max_replicas=6,
            current_cpu_req_m=500,
            current_cpu_lim_m=1000,
            current_mem_req_mi=768,
            current_mem_lim_mi=768,
            observed_avg_tps=1400,
            observed_peak_tps=5600,
            observed_p95_latency_ms=20,
            observed_cpu_ms_per_req=1.8,
            observed_avg_cpu_usage_m=890,  # Needs upscale for 5,600 RPS surge
            observed_peak_mem_usage_mi=710,
            has_db=True,
        ),
    ],
}


def _parse_k8s_cpu_to_m(val: str) -> int:
    """Convert Kubernetes CPU string ('500m', '1', '1.5') to millicores."""
    val = str(val).strip()
    if val.endswith("m"):
        return int(float(val[:-1]))
    if val.endswith("n"):
        return max(1, int(float(val[:-1]) / 1_000_000))
    return int(float(val) * 1000)


def _parse_k8s_mem_to_mi(val: str) -> int:
    """Convert Kubernetes Memory string ('512Mi', '1Gi', '536870912') to MiB."""
    val = str(val).strip()
    if val.endswith("Ki"):
        return max(1, int(float(val[:-2]) / 1024))
    if val.endswith("Mi"):
        return int(float(val[:-2]))
    if val.endswith("Gi"):
        return int(float(val[:-2]) * 1024)
    if val.endswith("G"):
        return int(float(val[:-1]) * 953.67)
    if val.endswith("M"):
        return int(float(val[:-1]) * 0.95367)
    return max(1, int(float(val) / (1024 * 1024)))


def fetch_live_k8s_deployments(
    namespace: str = "default",
    cloud_provider: str = "azure",
    prometheus_url: Optional[str] = None,
) -> List[LiveWorkloadTelemetry]:
    """
    Query the active kubeconfig context (`kubectl get deploy` + `kubectl top pods`)
    and optional Prometheus/Cloud Monitor endpoint to build LiveWorkloadTelemetry objects.
    """
    cmd = ["kubectl", "get", "deployments", "-n", namespace, "-o", "json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    payload = json.loads(proc.stdout)

    # Optionally pull live pod CPU/Memory usage via `kubectl top pods`
    usage_by_app: Dict[str, tuple[int, int]] = {}
    top_proc = subprocess.run(
        ["kubectl", "top", "pods", "-n", namespace, "--no-headers"],
        capture_output=True,
        text=True,
        check=False,
    )
    if top_proc.returncode == 0:
        for line in top_proc.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) >= 3:
                pod_name, cpu_str, mem_str = parts[0], parts[1], parts[2]
                usage_by_app[pod_name] = (_parse_k8s_cpu_to_m(cpu_str), _parse_k8s_mem_to_mi(mem_str))

    results: List[LiveWorkloadTelemetry] = []
    for item in payload.get("items", []):
        meta = item.get("metadata", {})
        spec = item.get("spec", {})
        svc_name = meta.get("name", "unknown-svc")
        replicas = int(spec.get("replicas", 2))
        containers = spec.get("template", {}).get("spec", {}).get("containers", [])
        if not containers:
            continue

        c0 = containers[0]
        res = c0.get("resources", {})
        reqs = res.get("requests", {})
        lims = res.get("limits", {})

        cpu_req_m = _parse_k8s_cpu_to_m(reqs.get("cpu", "500m"))
        cpu_lim_m = _parse_k8s_cpu_to_m(lims.get("cpu", str(cpu_req_m * 2) + "m"))
        mem_req_mi = _parse_k8s_mem_to_mi(reqs.get("memory", "512Mi"))
        mem_lim_mi = _parse_k8s_mem_to_mi(lims.get("memory", f"{mem_req_mi}Mi"))

        # Match any pod starting with deployment name in `kubectl top pods`
        matched_cpu = [v[0] for k, v in usage_by_app.items() if k.startswith(svc_name)]
        matched_mem = [v[1] for k, v in usage_by_app.items() if k.startswith(svc_name)]
        obs_cpu_m = int(sum(matched_cpu) / len(matched_cpu)) if matched_cpu else int(cpu_req_m * 0.6)
        obs_mem_mi = int(max(matched_mem)) if matched_mem else int(mem_req_mi * 0.7)

        # Estimate RPS from Prometheus if provided, otherwise infer from observed CPU
        obs_tps = max(20, int((obs_cpu_m * replicas) / 3.5))
        if prometheus_url:
            try:
                q_url = f"{prometheus_url.rstrip('/')}/api/v1/query?query=sum(rate(http_server_requests_seconds_count{{service='{svc_name}'}}[5m]))"
                with urllib.request.urlopen(q_url, timeout=3) as resp:
                    pdata = json.loads(resp.read().decode("utf-8"))
                    vec = pdata.get("data", {}).get("result", [])
                    if vec:
                        obs_tps = max(1, int(float(vec[0]["value"][1])))
            except Exception:
                pass

        results.append(
            LiveWorkloadTelemetry(
                cloud_provider=cloud_provider,
                cluster_name="kubeconfig-active-context",
                namespace=namespace,
                service=svc_name,
                runtime="jvm",
                current_replicas=replicas,
                current_max_replicas=max(replicas * 2, 4),
                current_cpu_req_m=cpu_req_m,
                current_cpu_lim_m=cpu_lim_m,
                current_mem_req_mi=mem_req_mi,
                current_mem_lim_mi=mem_lim_mi,
                observed_avg_tps=obs_tps,
                observed_peak_tps=obs_tps * 3,
                observed_p95_latency_ms=45,
                observed_cpu_ms_per_req=3.5,
                observed_avg_cpu_usage_m=obs_cpu_m,
                observed_peak_mem_usage_mi=obs_mem_mi,
                has_db=True,
            )
        )

    return results

