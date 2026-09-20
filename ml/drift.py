"""
Data Drift 탐지: PSI(Population Stability Index)로 피처 분포 변화를 측정한다.

기준 분포(baseline) 대비 현재 분포(current)가 얼마나 이동했는지를 수치화한다.
업계 통용 해석 (출처마다 편차 있음):
  PSI < 0.1        안정
  0.1 <= PSI < 0.2 주의
  PSI >= 0.2       재학습 검토

설계 결정 (2026-09-20 확정)
- 데이터 소스: creditcard.csv의 Time 순서 배치. 재학습 DAG와 동일 소스.
  운영 DB에는 V1~V28이 영속화되지 않아 모델이 실제 사용하는 피처의
  분포를 관측할 수 없음. 실서비스라면 운영 데이터에서 계산해야 함.
- 구간 분할: 기준 데이터의 10분위(quantile). 등간격은 Amount처럼
  이상치가 큰 피처에서 한 구간에 쏠림.
- 0 비율 보정: 어느 구간의 비율이 0이면 log 연산이 발산하므로
  최소값(EPSILON)으로 치환.
"""

import numpy as np

# 0으로 나누기/로그 발산 방지용 최소 비율
EPSILON = 1e-4

# 판정 임계값
PSI_WARN = 0.1
PSI_ALERT = 0.2


def build_baseline(values, n_bins=10):
    """
    기준 데이터로부터 구간 경계와 구간별 비율을 만든다.

    Args:
        values (array-like): 기준 데이터 (champion 모델의 학습 데이터)
        n_bins (int): 구간 개수. 기본 10 (10분위)

    Returns:
        dict: {"edges": [...], "ratios": [...]}
              edges 길이는 n_bins+1, ratios 길이는 n_bins
    """
    values = np.asarray(values, dtype=float)
    values = values[~np.isnan(values)]

    if values.size == 0:
        raise ValueError("기준 데이터가 비어 있습니다.")

    # 분위수로 구간 경계 생성
    quantiles = np.linspace(0, 100, n_bins + 1)
    edges = np.percentile(values, quantiles)

    # 동일 값이 많으면 경계가 중복될 수 있음 → 중복 제거
    edges = np.unique(edges)
    if edges.size < 2:
        raise ValueError("기준 데이터의 분산이 없어 구간을 나눌 수 없습니다.")

    # 양 끝을 무한대로 열어 현재 데이터가 범위를 벗어나도 수용
    edges[0] = -np.inf
    edges[-1] = np.inf

    counts, _ = np.histogram(values, bins=edges)
    ratios = counts / counts.sum()

    return {"edges": edges.tolist(), "ratios": ratios.tolist()}


def calculate_psi(baseline, current_values):
    """
    기준 분포 대비 현재 데이터의 PSI를 계산한다.

    PSI = Σ (current_ratio - baseline_ratio) * ln(current_ratio / baseline_ratio)

    Args:
        baseline (dict): build_baseline() 결과
        current_values (array-like): 현재 데이터

    Returns:
        float: PSI 값
    """
    edges = np.asarray(baseline["edges"], dtype=float)
    base_ratios = np.asarray(baseline["ratios"], dtype=float)

    current = np.asarray(current_values, dtype=float)
    current = current[~np.isnan(current)]

    if current.size == 0:
        raise ValueError("현재 데이터가 비어 있습니다.")

    counts, _ = np.histogram(current, bins=edges)
    curr_ratios = counts / counts.sum()

    # 0 비율 보정 — 로그 발산 방지
    base_ratios = np.where(base_ratios == 0, EPSILON, base_ratios)
    curr_ratios = np.where(curr_ratios == 0, EPSILON, curr_ratios)

    psi = np.sum((curr_ratios - base_ratios) * np.log(curr_ratios / base_ratios))
    return float(psi)


def judge_psi(psi, warn=PSI_WARN, alert=PSI_ALERT):
    """
    PSI 값을 3단계로 판정한다.

    Returns:
        (str, str): (등급, 설명)  등급은 "stable" | "warn" | "alert"
    """
    if psi < warn:
        return "stable", f"안정: PSI {psi:.4f} < {warn}"
    if psi < alert:
        return "warn", f"주의: PSI {psi:.4f} (기준 {warn} ~ {alert})"
    return "alert", f"경보: PSI {psi:.4f} >= {alert}. 재학습 검토 필요."


def calculate_psi_batch(baselines, current_df, features):
    """
    여러 피처의 PSI를 한 번에 계산한다.

    Args:
        baselines (dict): {feature_name: baseline_dict}
        current_df (pandas.DataFrame): 현재 데이터
        features (list[str]): 계산 대상 피처명

    Returns:
        dict: {feature_name: {"psi": float, "level": str, "reason": str}}
    """
    results = {}
    for feat in features:
        if feat not in baselines:
            raise KeyError(f"'{feat}' 의 baseline이 없습니다.")
        if feat not in current_df.columns:
            raise KeyError(f"'{feat}' 가 현재 데이터에 없습니다.")

        psi = calculate_psi(baselines[feat], current_df[feat].values)
        level, reason = judge_psi(psi)
        results[feat] = {"psi": psi, "level": level, "reason": reason}

    return results

def push_psi_metrics(results, gateway_url, job="sentinel_drift",
                     model_version=None):
    """
    PSI 결과를 Pushgateway로 전송한다.

    계산 로직과 분리된 I/O 함수. 단위 테스트 대상이 아님.

    Args:
        results (dict): calculate_psi_batch() 결과
        gateway_url (str): 예) "localhost:9091"
        job (str): Prometheus job 라벨
        model_version (str | None): 기준이 된 champion 버전
    """
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    registry = CollectorRegistry()

    psi_gauge = Gauge(
        "sentinel_feature_psi",
        "Population Stability Index per feature",
        ["feature", "model_version"],
        registry=registry,
    )
    level_gauge = Gauge(
        "sentinel_feature_psi_level",
        "PSI level (0=stable, 1=warn, 2=alert)",
        ["feature", "model_version"],
        registry=registry,
    )

    level_code = {"stable": 0, "warn": 1, "alert": 2}
    mv = model_version or "unknown"

    for feat, r in results.items():
        psi_gauge.labels(feature=feat, model_version=mv).set(r["psi"])
        level_gauge.labels(feature=feat, model_version=mv).set(level_code[r["level"]])

    push_to_gateway(gateway_url, job=job, registry=registry)