"""Core SRE capacity planning and Multi-Cloud FinOps estimation engine."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Dict, List


MEMORY_STEPS_MI: List[int] = [128, 256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144, 8192]

PRESETS: Dict[str, Dict[str, object]] = {
    "spring": {
        "service": "payment-core-api",
        "runtime": "jvm",
        "tps": 450,
        "burst": 3.0,
        "latency_ms": 45,
        "cpu_ms": 4.0,
        "payload_kb": 64,
        "slo": "99.99",
        "has_db": True,
    },
    "kafka": {
        "service": "activity-log-consumer",
        "runtime": "jvm",
        "tps": 1200,
        "burst": 6.0,
        "latency_ms": 15,
        "cpu_ms": 1.5,
        "payload_kb": 64,
        "slo": "99.99",
        "has_db": True,
    },
    "gateway": {
        "service": "edge-auth-proxy",
        "runtime": "go",
        "tps": 1800,
        "burst": 4.0,
        "latency_ms": 12,
        "cpu_ms": 0.8,
        "payload_kb": 16,
        "slo": "99.99",
        "has_db": False,
    },
    "python": {
        "service": "slack-incident-triage",
        "runtime": "python",
        "tps": 80,
        "burst": 2.5,
        "latency_ms": 180,
        "cpu_ms": 12.0,
        "payload_kb": 256,
        "slo": "99.9",
        "has_db": False,
    },
}


@dataclass(frozen=True)
class CloudTierEstimate:
    provider: str
    recommended_sku: str
    on_demand_usd: int
    reserved_1yr_usd: int
    committed_3yr_usd: int
    spot_burst_usd: int


@dataclass(frozen=True)
class SizingResult:
    service: str
    runtime: str
    tps: int
    peak_tps: int
    burst: float
    latency_ms: int
    cpu_ms: float
    payload_kb: int
    slo: str
    is_multi_az: bool
    has_db: bool
    base_concurrency: int
    peak_concurrency: int
    per_pod_concurrency: int
    total_base_cpu_m: int
    total_peak_cpu_m: int
    pod_cpu_req_m: int
    pod_cpu_lim_m: int
    pod_mem_req_mi: int
    pod_mem_lim_mi: int
    min_replicas: int
    max_replicas: int
    target_hpa_cpu_pct: int
    db_pool_per_pod: int
    max_cluster_db_conns: int
    total_vcpu_with_overhead: float
    total_mem_gib_with_overhead: float
    estimated_nodes: int
    clouds: List[CloudTierEstimate]

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def round_memory_step(mi: int) -> int:
    """Round raw memory MiB up to the nearest standard Kubernetes memory step."""
    for step in MEMORY_STEPS_MI:
        if mi <= step:
            return step
    return MEMORY_STEPS_MI[-1]


def analyze_workload(
    service: str = "payment-core-api",
    runtime: str = "jvm",
    tps: int = 450,
    burst: float = 3.0,
    latency_ms: int = 45,
    cpu_ms: float = 4.0,
    payload_kb: int = 64,
    slo: str = "99.99",
    has_db: bool = True,
) -> SizingResult:
    """
    Compute Kubernetes pod sizing, HPA bounds, HikariCP connection limits,
    and Multi-Cloud FinOps cost tiers using Little's Law and SRE best practices.
    """
    clean_name = "".join(c if c.isalnum() or c == "-" else "-" for c in service.lower()).strip("-") or "core-service"
    runtime_key = runtime.lower() if runtime.lower() in {"jvm", "go", "node", "python"} else "jvm"
    tps = max(1, int(tps))
    burst = max(1.0, float(burst))
    latency_ms = max(1, int(latency_ms))
    cpu_ms = max(0.1, float(cpu_ms))
    payload_kb = max(1, int(payload_kb))
    is_multi_az = slo.startswith("99.99")

    peak_tps = int(round(tps * burst))

    # 1. Little's Law Concurrency: L = lambda * W
    base_concurrency = max(1, math.ceil(tps * (latency_ms / 1000.0)))
    peak_concurrency = max(2, math.ceil(peak_tps * (latency_ms / 1000.0)))

    # 2. CPU Sizing (Target 65% utilization at baseline so HPA triggers before CFS throttling)
    target_hpa_cpu = 65
    total_base_cpu_m = math.ceil((tps * cpu_ms) / (target_hpa_cpu / 100.0))
    total_peak_cpu_m = math.ceil((peak_tps * cpu_ms) / (target_hpa_cpu / 100.0))

    if total_base_cpu_m <= 600:
        pod_cpu_req_m = 250
    elif total_base_cpu_m <= 2400:
        pod_cpu_req_m = 500
    elif total_base_cpu_m <= 8000:
        pod_cpu_req_m = 1000
    else:
        pod_cpu_req_m = 2000

    pod_cpu_lim_m = pod_cpu_req_m * 2

    # 3. Replica Counts (Enforce multi-AZ minimum + N+1 zone failure redundancy)
    min_az_pods = 3 if is_multi_az else 2
    raw_min_replicas = math.ceil(total_base_cpu_m / pod_cpu_req_m) + (1 if is_multi_az else 0)
    min_replicas = max(min_az_pods, raw_min_replicas)
    max_replicas = max(
        min_replicas + 2,
        math.ceil(total_peak_cpu_m / pod_cpu_req_m) + (2 if is_multi_az else 1),
    )

    # 4. Guaranteed Memory QoS (requests.memory == limits.memory prevents burstable OOM eviction)
    per_pod_concurrency = max(2, math.ceil(peak_concurrency / min_replicas))
    base_mem_map = {"jvm": 512, "python": 256, "node": 192, "go": 96}
    overhead_mult = {"jvm": 1.35, "python": 1.25, "node": 1.25, "go": 1.15}

    active_buffer_mi = math.ceil((per_pod_concurrency * payload_kb * 4) / 1024.0)
    raw_req_mi = math.ceil((base_mem_map[runtime_key] + active_buffer_mi) * overhead_mult[runtime_key])
    pod_mem_req_mi = round_memory_step(raw_req_mi)
    pod_mem_lim_mi = pod_mem_req_mi

    # 5. HikariCP / DB Connection Pool Guardrail
    pod_cores = max(1, math.ceil(pod_cpu_req_m / 1000.0))
    ideal_hikari = max(6, min(20, (pod_cores * 4) + math.ceil(per_pod_concurrency * 0.25)))
    db_pool_per_pod = ideal_hikari if has_db else 0
    max_cluster_db_conns = db_pool_per_pod * max_replicas

    # 6. Multi-Cloud FinOps Cost Estimation (730 hrs/mo + 20% burst duty cycle + 18% Kubelet/DaemonSet overhead)
    avg_active_pods = min_replicas + (max_replicas - min_replicas) * 0.20
    total_vcpu = ((avg_active_pods * pod_cpu_req_m) / 1000.0) * 1.18
    total_mem_gib = ((avg_active_pods * pod_mem_req_mi) / 1024.0) * 1.18

    sku_idx = 0 if total_vcpu <= 6 else (1 if total_vcpu <= 16 else 2)
    node_cores = [2, 4, 8][sku_idx]
    est_nodes = max(3 if is_multi_az else 2, math.ceil(total_vcpu / (node_cores * 0.82)))

    aws_sku = ["m6i.large (2c/8G)", "m6i.xlarge (4c/16G)", "m6i.2xlarge (8c/32G)"][sku_idx]
    az_sku = ["Standard_D2s_v5 (2c/8G)", "Standard_D4s_v5 (4c/16G)", "Standard_D8s_v5 (8c/32G)"][sku_idx]
    gcp_sku = ["n2-standard-2 (2c/8G)", "n2-standard-4 (4c/16G)", "n2-standard-8 (8c/32G)"][sku_idx]

    aws_base = int(round(total_vcpu * 30.3 + total_mem_gib * 4.0))
    az_base = int(round(total_vcpu * 29.2 + total_mem_gib * 3.85))
    gcp_base = int(round(total_vcpu * 27.8 + total_mem_gib * 3.70))

    clouds = [
        CloudTierEstimate(
            provider="AWS EKS",
            recommended_sku=aws_sku,
            on_demand_usd=aws_base,
            reserved_1yr_usd=int(round(aws_base * 0.64)),
            committed_3yr_usd=int(round(aws_base * 0.44)),
            spot_burst_usd=int(round(aws_base * 0.31)),
        ),
        CloudTierEstimate(
            provider="Azure AKS",
            recommended_sku=az_sku,
            on_demand_usd=az_base,
            reserved_1yr_usd=int(round(az_base * 0.62)),
            committed_3yr_usd=int(round(az_base * 0.42)),
            spot_burst_usd=int(round(az_base * 0.29)),
        ),
        CloudTierEstimate(
            provider="GCP GKE",
            recommended_sku=gcp_sku,
            on_demand_usd=gcp_base,
            reserved_1yr_usd=int(round(gcp_base * 0.63)),
            committed_3yr_usd=int(round(gcp_base * 0.43)),
            spot_burst_usd=int(round(gcp_base * 0.28)),
        ),
    ]

    return SizingResult(
        service=clean_name,
        runtime=runtime_key,
        tps=tps,
        peak_tps=peak_tps,
        burst=burst,
        latency_ms=latency_ms,
        cpu_ms=cpu_ms,
        payload_kb=payload_kb,
        slo="99.99" if is_multi_az else "99.9",
        is_multi_az=is_multi_az,
        has_db=has_db,
        base_concurrency=base_concurrency,
        peak_concurrency=peak_concurrency,
        per_pod_concurrency=per_pod_concurrency,
        total_base_cpu_m=total_base_cpu_m,
        total_peak_cpu_m=total_peak_cpu_m,
        pod_cpu_req_m=pod_cpu_req_m,
        pod_cpu_lim_m=pod_cpu_lim_m,
        pod_mem_req_mi=pod_mem_req_mi,
        pod_mem_lim_mi=pod_mem_lim_mi,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        target_hpa_cpu_pct=target_hpa_cpu,
        db_pool_per_pod=db_pool_per_pod,
        max_cluster_db_conns=max_cluster_db_conns,
        total_vcpu_with_overhead=round(total_vcpu, 2),
        total_mem_gib_with_overhead=round(total_mem_gib, 2),
        estimated_nodes=est_nodes,
        clouds=clouds,
    )

