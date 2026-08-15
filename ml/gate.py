"""
성능 게이트: 재학습된 challenger 모델을 현행 champion과 비교해
배포 승인/차단을 판정한다.

판정 지표: ROC-AUC (2026-08-07 확정)
규칙: challenger_roc_auc >= champion_roc_auc - tolerance → 통과
tolerance: 0.0 (프로덕션 학습이 결정론적임을 실측 규명 → 노이즈 마진 불필요)
"""


def evaluate_gate(challenger_roc_auc, champion_roc_auc=None, tolerance=0.0):
    """
    challenger가 배포 자격이 있는지 판정한다.

    Args:
        challenger_roc_auc (float): 새로 재학습한 모델의 holdout ROC-AUC
        champion_roc_auc (float | None): 현행 모델의 ROC-AUC.
                                          None이면 champion 없음(최초 학습).
        tolerance (float): 허용 하락 폭. 기본 0.0.

    Returns:
        (bool, str): (통과 여부, 판정 이유)
    """
    # 최초 학습 — 비교 대상 없음 → 첫 champion으로 등록
    if champion_roc_auc is None:
        return True, (
            f"champion 없음(최초 학습). "
            f"challenger {challenger_roc_auc:.4f} 를 첫 champion으로 등록."
        )

    threshold = champion_roc_auc - tolerance

    if challenger_roc_auc >= threshold:
        return True, (
            f"통과: challenger {challenger_roc_auc:.4f} "
            f">= champion {champion_roc_auc:.4f} - tol {tolerance} "
            f"(기준 {threshold:.4f})."
        )
    else:
        return False, (
            f"차단: challenger {challenger_roc_auc:.4f} "
            f"< champion {champion_roc_auc:.4f} - tol {tolerance} "
            f"(기준 {threshold:.4f}). 성능 열화로 배포 중단."
        )