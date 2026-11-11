"""Live Cloud & Kubernetes Right-Sizing Auditor (Upscale / Downscale / Cost Waste Detector)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Dict, List

from .cloud_connectors import LiveWorkloadTelemetry
from .engine import SizingResult, analyze_workload


# Published monthly on-demand rates ($/vCPU-mo and $/GiB-mo on 730h basis)
CLOUD_RATES = {
    "aws": (30.3, 4.00),
    "azure": (29.2, 3.85),
    "gcp": (27.8, 3.70),
}


@dataclass(frozen=True)
class AuditRecommendation:
    cloud_provider: str
    cluster_name: str
    namespace: str
    service: str
    action: str                     # "UPSCALE_URGENT" | "DOWNSCALE_SAVE" | "RIGHT_SIZED"
    reason: str
    current_replicas: str
    recommended_replicas: str
    current_cpu_req: str
    recommended_cpu_req: str
    current_mem_req: str
    recommended_mem_req: str
    current_monthly_usd: int
    recommended_monthly_usd: int
    monthly_delta_usd: int          # Negative = Savings ($ saved/mo), Positive = Investment needed
    kubectl_patch_cmd: str
    recommended_sizing: SizingResult

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def audit_live_workload(telemetry: LiveWorkloadTelemetry, slo: str = "99.99") -> AuditRecommendation:
    """
    Compare a live AWS EKS, Azure AKS, or GCP GKE workload's current allocation
    against its observed traffic telemetry and recommend UPSCALE, DOWNSCALE, or RIGHT_SIZED.
    """
    burst_mult = max(1.5, round(telemetry.observed_peak_tps / max(1, telemetry.observed_avg_tps), 1))
    ideal = analyze_workload(
        service=telemetry.service,
        runtime=telemetry.runtime,
        tps=telemetry.observed_avg_tps,
        burst=burst_mult,
        latency_ms=telemetry.observed_p95_latency_ms,
        cpu_ms=telemetry.observed_cpu_ms_per_req,
        payload_kb=64,
        slo=slo,
        has_db=telemetry.has_db,
    )

    vcpu_rate, gib_rate = CLOUD_RATES.get(telemetry.cloud_provider.lower(), CLOUD_RATES["azure"])

    # Current monthly cost (including 18% Kubelet/DaemonSet overhead)
    curr_vcpu = ((telemetry.current_replicas * telemetry.current_cpu_req_m) / 1000.0) * 1.18
    curr_gib = ((telemetry.current_replicas * telemetry.current_mem_req_mi) / 1024.0) * 1.18
    curr_monthly_usd = int(round(curr_vcpu * vcpu_rate + curr_gib * gib_rate))

    # Recommended monthly cost
    rec_vcpu = ((ideal.min_replicas * ideal.pod_cpu_req_m) / 1000.0) * 1.18
    rec_gib = ((ideal.min_replicas * ideal.pod_mem_req_mi) / 1024.0) * 1.18
    rec_monthly_usd = int(round(rec_vcpu * vcpu_rate + rec_gib * gib_rate))
    delta_usd = rec_monthly_usd - curr_monthly_usd

    curr_total_cpu_m = telemetry.current_replicas * telemetry.current_cpu_req_m
    ideal_total_cpu_m = ideal.min_replicas * ideal.pod_cpu_req_m

    # Determine UPSCALE vs DOWNSCALE vs RIGHT_SIZED
    cpu_util_pct = int(round((telemetry.observed_avg_cpu_usage_m / max(1, telemetry.current_cpu_req_m)) * 100))
    if ideal_total_cpu_m > curr_total_cpu_m * 1.20 or cpu_util_pct > 85:
        action = "UPSCALE_URGENT"
        reason = (
            f"Pods running at {cpu_util_pct}% of CPU request ({telemetry.observed_avg_cpu_usage_m}m used vs "
            f"{telemetry.current_cpu_req_m}m req) under {telemetry.observed_avg_tps}->{telemetry.observed_peak_tps} RPS. "
            f"High CFS throttling & SLO breach risk."
        )
    elif ideal_total_cpu_m < curr_total_cpu_m * 0.75:
        action = "DOWNSCALE_SAVE"
        saved = abs(delta_usd)
        reason = (
            f"Over-provisioned ({cpu_util_pct}% avg CPU utilization at {telemetry.observed_avg_tps} RPS). "
            f"Right-sizing reclaims {curr_total_cpu_m - ideal_total_cpu_m}m CPU and saves ~${saved}/mo."
        )
    else:
        action = "RIGHT_SIZED"
        reason = f"Allocation matches observed {telemetry.observed_avg_tps} RPS traffic within +/-20% tolerance."

    patch_json = json.dumps(
        {
            "spec": {
                "replicas": ideal.min_replicas,
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": telemetry.service,
                                "resources": {
                                    "requests": {
                                        "cpu": f"{ideal.pod_cpu_req_m}m",
                                        "memory": f"{ideal.pod_mem_req_mi}Mi",
                                    },
                                    "limits": {
                                        "cpu": f"{ideal.pod_cpu_lim_m}m",
                                        "memory": f"{ideal.pod_mem_lim_mi}Mi",
                                    },
                                },
                            }
                        ]
                    }
                },
            }
        }
    )
    patch_cmd = f"kubectl patch deploy {telemetry.service} -n {telemetry.namespace} -p '{patch_json}'"

    return AuditRecommendation(
        cloud_provider=telemetry.cloud_provider.upper(),
        cluster_name=telemetry.cluster_name,
        namespace=telemetry.namespace,
        service=telemetry.service,
        action=action,
        reason=reason,
        current_replicas=f"{telemetry.current_replicas} -> {telemetry.current_max_replicas}",
        recommended_replicas=f"{ideal.min_replicas} -> {ideal.max_replicas}",
        current_cpu_req=f"{telemetry.current_cpu_req_m}m",
        recommended_cpu_req=f"{ideal.pod_cpu_req_m}m",
        current_mem_req=f"{telemetry.current_mem_req_mi}Mi",
        recommended_mem_req=f"{ideal.pod_mem_req_mi}Mi",
        current_monthly_usd=curr_monthly_usd,
        recommended_monthly_usd=rec_monthly_usd,
        monthly_delta_usd=delta_usd,
        kubectl_patch_cmd=patch_cmd,
        recommended_sizing=ideal,
    )


def format_audit_report(recs: List[AuditRecommendation]) -> str:
    """Format a terminal table of cloud upscale/downscale recommendations."""
    if not recs:
        return "No workloads found to audit."

    first = recs[0]
    lines = [
        "=" * 86,
        f" LIVE CLOUD TRAFFIC & RIGHT-SIZING AUDIT :: {first.cloud_provider} ({first.cluster_name})",
        "=" * 86,
    ]
    total_curr = 0
    total_rec = 0

    for r in recs:
        total_curr += r.current_monthly_usd
        total_rec += r.recommended_monthly_usd
        delta_str = (
            f"-${abs(r.monthly_delta_usd)}/mo (SAVE)"
            if r.monthly_delta_usd < 0
            else f"+${r.monthly_delta_usd}/mo (PROTECT SLO)"
        )
        lines.extend(
            [
                f" SERVICE : {r.namespace}/{r.service}   --->   [{r.action}]",
                f" Reason  : {r.reason}",
                f" Diff    : Replicas [{r.current_replicas}] => [{r.recommended_replicas}]  |  "
                f"CPU [{r.current_cpu_req}] => [{r.recommended_cpu_req}]  |  "
                f"Mem [{r.current_mem_req}] => [{r.recommended_mem_req}]",
                f" FinOps  : ${r.current_monthly_usd}/mo current => ${r.recommended_monthly_usd}/mo recommended ({delta_str})",
                f" Patch   : {r.kubectl_patch_cmd}",
                "-" * 86,
            ]
        )

    net_diff = total_rec - total_curr
    net_label = f"NET MONTHLY SAVINGS: ${abs(net_diff)}/mo" if net_diff <= 0 else f"NET SLO INVESTMENT: +${net_diff}/mo"
    lines.append(f" SUMMARY : Current Cluster Spend: ${total_curr}/mo  |  Right-Sized Spend: ${total_rec}/mo  ({net_label})")
    lines.append("=" * 86)
    return "\n".join(lines)
