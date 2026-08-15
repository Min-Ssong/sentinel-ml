"""
gate.evaluate_gate 단위 테스트.
통과/차단/최초학습 세 경로를 모두 검증한다(both-path + 경계).
실행: pytest test_gate.py -v   또는   python test_gate.py
"""

from gate import evaluate_gate


def test_champion_없으면_통과():
    passed, reason = evaluate_gate(challenger_roc_auc=0.9623, champion_roc_auc=None)
    assert passed is True
    print(f"[최초학습] {reason}")


def test_challenger가_더_높으면_통과():
    # 재학습1: 0.9688 vs champion 0.9623 → 통과 (실측 시나리오)
    passed, reason = evaluate_gate(0.9688, 0.9623)
    assert passed is True
    print(f"[개선]     {reason}")


def test_challenger가_같으면_통과():
    # 동점은 통과 (>= 기준). tolerance 0.0에서 경계값 검증
    passed, reason = evaluate_gate(0.9688, 0.9688)
    assert passed is True
    print(f"[동점]     {reason}")


def test_challenger가_더_낮으면_차단():
    # 의도적 열화 challenger → 게이트가 막아야 함 (차단 경로 검증)
    passed, reason = evaluate_gate(0.9500, 0.9688)
    assert passed is False
    print(f"[열화차단] {reason}")


def test_미세_하락도_차단_tolerance_0():
    # tolerance 0.0이므로 0.0001 하락도 차단 (결정론적 시스템 = 진짜 열화)
    passed, reason = evaluate_gate(0.9687, 0.9688)
    assert passed is False
    print(f"[미세하락] {reason}")


if __name__ == "__main__":
    # pytest 없이도 직접 실행 가능하게
    test_champion_없으면_통과()
    test_challenger가_더_높으면_통과()
    test_challenger가_같으면_통과()
    test_challenger가_더_낮으면_차단()
    test_미세_하락도_차단_tolerance_0()
    print("\n전체 통과 ✅")