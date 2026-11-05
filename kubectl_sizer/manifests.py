"""Kubernetes YAML & Spring Boot / HikariCP configuration manifest generators."""

from __future__ import annotations

from typing import Dict
from .engine import SizingResult


def generate_deployment_yaml(r: SizingResult) -> str:
    """Generate a PodSecurityStandards (restricted) compliant Kubernetes Deployment manifest."""
    topology_block = ""
    if r.is_multi_az:
        topology_block = f"""
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              app.kubernetes.io/name: {r.service}"""

    jvm_env_block = ""
    if r.runtime == "jvm":
        jvm_env_block = """
          env:
            - name: JAVA_TOOL_OPTIONS
              value: >-
                -XX:+UseContainerSupport
                -XX:InitialRAMPercentage=75.0
                -XX:MaxRAMPercentage=75.0
                -XX:+UseG1GC
                -XX:+ExitOnOutOfMemoryError
                -Dsun.net.inetaddr.ttl=30"""

    return f"""# Enterprise K8s Deployment · {r.service} (PSS Restricted Compliant)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {r.service}
  namespace: prod-workloads
  labels:
    app.kubernetes.io/name: {r.service}
    sre.kunj.site/slo-tier: "{r.slo}"
spec:
  replicas: {r.min_replicas}
  revisionHistoryLimit: 5
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 25%
      maxUnavailable: 0
  selector:
    matchLabels:
      app.kubernetes.io/name: {r.service}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: {r.service}
    spec:
      terminationGracePeriodSeconds: 45
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        seccompProfile:
          type: RuntimeDefault{topology_block}
      containers:
        - name: {r.service}
          image: registry.prod.internal/{r.service}:v1.4.2
          ports:
            - name: http
              containerPort: 8080
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          resources:
            requests:
              cpu: "{r.pod_cpu_req_m}m"
              memory: "{r.pod_mem_req_mi}Mi"
            limits:
              cpu: "{r.pod_cpu_lim_m}m"
              memory: "{r.pod_mem_lim_mi}Mi"   # Guaranteed Memory (Req == Limit prevents OOM eviction){jvm_env_block}
          startupProbe:
            httpGet:
              path: /actuator/health/liveness
              port: http
            failureThreshold: 30
            periodSeconds: 2
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: http
            periodSeconds: 5
            failureThreshold: 2
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: http
            periodSeconds: 10
            failureThreshold: 3
"""


def generate_hpa_pdb_yaml(r: SizingResult) -> str:
    """Generate HorizontalPodAutoscaler v2 and PodDisruptionBudget manifests."""
    min_avail = max(1, r.min_replicas - 1)
    return f"""# HPA v2 + PodDisruptionBudget · {r.tps} -> {r.peak_tps} RPS ({r.burst}x Surge)
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {r.service}-hpa
  namespace: prod-workloads
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {r.service}
  minReplicas: {r.min_replicas}
  maxReplicas: {r.max_replicas}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {r.target_hpa_cpu_pct}
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 15
      policies:
        - type: Percent
          value: 50
          periodSeconds: 30
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Pods
          value: 1
          periodSeconds: 60
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {r.service}-pdb
  namespace: prod-workloads
spec:
  minAvailable: {min_avail}
  selector:
    matchLabels:
      app.kubernetes.io/name: {r.service}
"""


def generate_app_config_yaml(r: SizingResult) -> str:
    """Generate tuned Spring Boot / HikariCP / HTTP pool runtime configuration."""
    server_threads = max(20, min(200, r.per_pod_concurrency * 4))
    min_spare = max(10, round(server_threads / 4))

    if r.has_db:
        min_idle = max(4, round(r.db_pool_per_pod / 2))
        pool_section = f"""spring:
  datasource:
    hikari:
      pool-name: {r.service}-hikari
      maximum-pool-size: {r.db_pool_per_pod}          # Max {r.max_cluster_db_conns} total DB conns @ {r.max_replicas} pods
      minimum-idle: {min_idle}
      connection-timeout: 10000      # Fail fast (10s) to prevent thread starvation
      keepalive-time: 120000         # Active 2m probe evicts dead firewall sockets
      max-lifetime: 600000           # Recycle connections every 10m
      validation-timeout: 3000

# JVM DNS Cache Policy (Prevents stale IP blackholing after DB failover)
networkaddress.cache.ttl: 30
networkaddress.cache.negative.ttl: 5"""
    else:
        max_route = max(32, r.per_pod_concurrency * 2)
        max_total = max(128, r.per_pod_concurrency * 6)
        read_timeout = min(10000, r.latency_ms * 4)
        pool_section = f"""# Stateless Worker / Upstream HTTP Client Pool Tuning
http:
  client:
    pool:
      max-connections-per-route: {max_route}
      max-total-connections: {max_total}
      connect-timeout-ms: 2000
      read-timeout-ms: {read_timeout}
      evict-idle-connections-sec: 60"""

    return f"""# Tuned Runtime & Connection Pool Config · {r.service}
# Sizing Target: {r.min_replicas}..{r.max_replicas} pods · {r.per_pod_concurrency} concurrent reqs/pod
server:
  port: 8080
  shutdown: graceful
  tomcat:
    threads:
      max: {server_threads}
      min-spare: {min_spare}
    accept-count: 100
    connection-timeout: 5000ms

{pool_section}
"""


def generate_finops_yaml(r: SizingResult) -> str:
    """Generate a Multi-Cloud FinOps cost & capacity report across AWS, Azure, and GCP."""
    aws, az, gcp = r.clouds[0], r.clouds[1], r.clouds[2]
    slo_desc = "99.99% Multi-AZ Active-Active" if r.is_multi_az else "99.9% Standard HA"
    return f"""# Multi-Cloud FinOps Cost & Capacity Report · {r.service}
# Workload Footprint: {r.min_replicas} baseline -> {r.max_replicas} peak pods ({r.total_vcpu_with_overhead:.2f} vCPU · {r.total_mem_gib_with_overhead:.2f} GiB avg)
finopsAnalysis:
  service: {r.service}
  sloTier: "{slo_desc}"
  estimatedNodePoolCount: {r.estimated_nodes}
  littlesLawConcurrency:
    baselineInFlightRequests: {r.base_concurrency}
    peakInFlightRequests: {r.peak_concurrency}
    perPodConcurrencyTarget: {r.per_pod_concurrency}
  podSizing:
    cpuRequest: "{r.pod_cpu_req_m}m"
    cpuLimit: "{r.pod_cpu_lim_m}m"
    memoryRequest: "{r.pod_mem_req_mi}Mi"
    memoryLimit: "{r.pod_mem_lim_mi}Mi"

monthlyCloudEstimatesUSD:
  # Workload share (730h/mo + 18% DaemonSet/Kubelet overhead; excludes $73/mo cluster control-plane fee)
  aws_eks:
    recommendedInstance: "{aws.recommended_sku}"
    tiers:
      onDemand_PayAsYouGo: "${aws.on_demand_usd}/mo"
      savingsPlan_1Yr:     "${aws.reserved_1yr_usd}/mo   # -36% vs On-Demand (Recommended Baseline)"
      savingsPlan_3Yr:     "${aws.committed_3yr_usd}/mo   # -56% vs On-Demand"
      ec2Spot_BurstPool:   "${aws.spot_burst_usd}/mo   # -69% vs On-Demand (Stateless HPA Surge)"

  azure_aks:
    recommendedInstance: "{az.recommended_sku}"
    tiers:
      payAsYouGo:          "${az.on_demand_usd}/mo"
      reserved_1Yr:        "${az.reserved_1yr_usd}/mo   # -38% vs Pay-As-You-Go (Recommended Baseline)"
      reserved_3Yr:        "${az.committed_3yr_usd}/mo   # -58% vs Pay-As-You-Go"
      azureSpot_ScaleSet:  "${az.spot_burst_usd}/mo   # -71% vs Pay-As-You-Go (Stateless HPA Surge)"

  gcp_gke:
    recommendedInstance: "{gcp.recommended_sku}"
    tiers:
      onDemand:            "${gcp.on_demand_usd}/mo"
      committedUse_1Yr:    "${gcp.reserved_1yr_usd}/mo   # -37% vs On-Demand (Recommended Baseline)"
      committedUse_3Yr:    "${gcp.committed_3yr_usd}/mo   # -57% vs On-Demand"
      spotVMs_Preemptible: "${gcp.spot_burst_usd}/mo   # -72% vs On-Demand (Stateless HPA Surge)"
"""


def generate_all_manifests(r: SizingResult) -> Dict[str, str]:
    """Return all four generated YAML files keyed by filename."""
    return {
        "deployment.yaml": generate_deployment_yaml(r),
        "hpa-pdb.yaml": generate_hpa_pdb_yaml(r),
        "application-prod.yml": generate_app_config_yaml(r),
        "finops-cost.yaml": generate_finops_yaml(r),
    }

