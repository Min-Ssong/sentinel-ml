"""
sentinel_drift_monitor 판정 규칙 단위 테스트 (_window_hours, _hold_reasons, decide).

검증 대상은 DAG 파일의 판정 함수 본문이다. Airflow 의 @task / @dag 데코레이터와
TriggerDagRunOperator 는 가짜로 바꿔 끼워 Airflow 설치 없이 실행한다.
  - 검증하지 않는 것: Airflow 가 short_circuit 반환값을 받아 하위 태스크를 스킵하는 동작, DAG 연결 구조
  - MLflow / Prometheus / FastAPI 호출(check_cooldown, fetch_recall 등)은 대상이 아니다.
    decide 는 그 결과(dict)를 입력으로 받는 순수 판정이므로 입력 dict 를 직접 만들어 넣는다.

실행 (저장소 루트에서):  pytest infra/docker/airflow/tests -v
기대값의 근거는 DAG 파일 상단 docstring 의 판정 규칙과 _hold_reasons / decide 코드다.
"""
import importlib.util
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "sentinel_drift_monitor.py"


def _fake_airflow_modules():
    class _FakeTask:
        """@task, @task.short_circuit → 데코레이션한 함수를 그대로 돌려준다."""

        def __call__(self, fn=None, **_kw):
            return fn if fn is not None else (lambda f: f)

        def short_circuit(self, fn=None, **_kw):
            return fn if fn is not None else (lambda f: f)

    def fake_dag(**_kw):
        # 모듈 끝의 `dag_instance = sentinel_drift_monitor()` 가 실제 태스크를 실행하지 않도록 빈 함수로 바꾼다
        return lambda _fn: (lambda *a, **k: None)

    sdk = types.ModuleType("airflow.sdk")
    sdk.dag, sdk.task = fake_dag, _FakeTask()
    trigger = types.ModuleType("airflow.providers.standard.operators.trigger_dagrun")
    trigger.TriggerDagRunOperator = lambda **_kw: None

    mods = {"airflow": types.ModuleType("airflow"), "airflow.sdk": sdk}
    for name in ("airflow.providers", "airflow.providers.standard", "airflow.providers.standard.operators"):
        mods[name] = types.ModuleType(name)
    mods["airflow.providers.standard.operators.trigger_dagrun"] = trigger
    return mods


@pytest.fixture(scope="module")
def dag():
    with mock.patch.dict(sys.modules, _fake_airflow_modules()):
        spec = importlib.util.spec_from_file_location("sentinel_drift_monitor_under_test", DAG_FILE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


# ---------- 입력 만들기 ----------

def cooldown(**override):
    """check_cooldown 이 돌려주는 dict. 기본값 = 보류 사유 없는 정상 상태."""
    base = {"serving_version": "12", "champion_version": "12", "challenger_version": None,
            "last_drift_age_h": None, "last_alias_change": None, "last_alias_change_age_h": None,
            "errors": []}
    base.update(override)
    return base


def psi(level="stable"):
    return {"current_csv": "creditcard.csv", "model_version": "v12", "pushed": True,
            "features": {"V14": {"psi": 0.05, "level": level}, "Amount": {"psi": 0.05, "level": level}}}


def recall(value, samples):
    return {"window": "1h", "tp": 0, "fn": 0, "samples": samples, "recall": value}


def params(dag_module, **override):
    p = dict(dag_module.DEFAULT_PARAMS)  # recall_threshold 0.60, min_samples 100, window 1h, cooldown_hours 2
    p.update(override)
    return p


def run_decide(dag_module, rec, cd, psi_result=None, **param_override):
    return dag_module.decide(psi_result or psi(), rec, cd, params=params(dag_module, **param_override))


# ---------- _window_hours ----------

@pytest.mark.parametrize("text, expected", [
    ("1h", 1), ("2h", 2), ("30m", 0.5), ("90m", 1.5), ("1d", 24), ("45s", 45 / 3600),
])
def test_window_hours_단위_변환(dag, text, expected):
    assert dag._window_hours(text) == pytest.approx(expected)


@pytest.mark.parametrize("bad", ["", "1", "h", "1w", "1.5h", "-1h", "1 h", "1H"])
def test_window_hours_형식_오류는_예외(dag, bad):
    with pytest.raises(ValueError):
        dag._window_hours(bad)


# ---------- _hold_reasons: 고정 보류 ----------

def test_보류_없음(dag):
    assert dag._hold_reasons(cooldown(), 2, 1) == ([], [])


def test_서빙_버전_조회_실패는_고정_보류(dag):
    hard, soft = dag._hold_reasons(cooldown(serving_version=None), 2, 1)
    assert len(hard) == 1 and "서빙 버전 조회 실패" in hard[0]
    assert soft == []


def test_champion_조회_실패는_고정_보류(dag):
    hard, soft = dag._hold_reasons(cooldown(champion_version=None), 2, 1)
    assert len(hard) == 1 and "champion 조회 실패" in hard[0]
    assert soft == []


def test_서빙과_champion_둘_다_조회_실패해도_사유는_서빙_조회_실패_하나(dag):
    # 코드가 if / elif 체인이라 첫 사유만 남는다
    hard, _ = dag._hold_reasons(cooldown(serving_version=None, champion_version=None), 2, 1)
    assert len(hard) == 1 and "서빙 버전 조회 실패" in hard[0]


def test_서빙_버전과_champion_불일치는_고정_보류(dag):
    hard, soft = dag._hold_reasons(cooldown(serving_version="16", champion_version="12"), 2, 1)
    assert len(hard) == 1 and "서빙 반영 대기" in hard[0]
    assert "v16" in hard[0] and "v12" in hard[0]
    assert soft == []


def test_챌린저_검증_중이면_고정_보류(dag):
    hard, soft = dag._hold_reasons(cooldown(challenger_version="18"), 2, 1)
    assert len(hard) == 1 and "챌린저 검증 중" in hard[0] and "v18" in hard[0]
    assert soft == []


def test_챌린저_있고_서빙_불일치면_고정_보류_둘_다(dag):
    hard, _ = dag._hold_reasons(cooldown(serving_version="16", challenger_version="18"), 2, 1)
    assert len(hard) == 2


# ---------- _hold_reasons: 시간 보류 (쿨다운) ----------

def test_쿨다운_기준_미만이면_시간_보류(dag):
    hard, soft = dag._hold_reasons(cooldown(last_drift_age_h=1.99), 2, 1)
    assert hard == [] and len(soft) == 1 and "쿨다운" in soft[0]


def test_쿨다운_경계_같으면_보류_아님(dag):
    # age < cooldown_hours (같으면 풀림)
    assert dag._hold_reasons(cooldown(last_drift_age_h=2.0), 2, 1) == ([], [])


def test_쿨다운_직전_재학습_없으면_보류_아님(dag):
    assert dag._hold_reasons(cooldown(last_drift_age_h=None), 2, 1) == ([], [])


def test_쿨다운_age_0도_보류(dag):
    # `if age is not None` 로 검사하므로 0 은 "없음" 이 아니라 "방금" 이다
    _, soft = dag._hold_reasons(cooldown(last_drift_age_h=0), 2, 1)
    assert len(soft) == 1


def test_쿨다운_0이면_어떤_age도_보류_아님(dag):
    # 10/5 사고 조건 (cooldown_hours=0 → 쿨다운 무력화)을 코드가 그대로 따르는지 확인
    assert dag._hold_reasons(cooldown(last_drift_age_h=0), 0, 1) == ([], [])


# ---------- _hold_reasons: 시간 보류 (alias 변경 직후, 보류 조건 ⑤) ----------

def test_alias_변경_후_판정_창_미만이면_시간_보류(dag):
    hard, soft = dag._hold_reasons(
        cooldown(last_alias_change="rejected_at@v18", last_alias_change_age_h=0.5), 2, 1)
    assert hard == [] and len(soft) == 1
    assert "alias 변경 직후" in soft[0] and "rejected_at@v18" in soft[0]


def test_alias_변경_경계_창과_같으면_보류_아님(dag):
    assert dag._hold_reasons(cooldown(last_alias_change="x@v1", last_alias_change_age_h=1.0), 2, 1) == ([], [])


def test_alias_변경_age_0도_보류(dag):
    _, soft = dag._hold_reasons(cooldown(last_alias_change="x@v1", last_alias_change_age_h=0), 2, 1)
    assert len(soft) == 1


def test_alias_변경_이력_없으면_보류_아님(dag):
    assert dag._hold_reasons(cooldown(last_alias_change_age_h=None), 2, 1) == ([], [])


def test_window_hours_None이면_alias_보류_검사_안_함(dag):
    assert dag._hold_reasons(cooldown(last_alias_change="x@v1", last_alias_change_age_h=0.1), 2, None) == ([], [])


def test_alias_변경_키가_없는_옛_형식_입력도_동작(dag):
    cd = cooldown()
    del cd["last_alias_change_age_h"], cd["last_alias_change"], cd["challenger_version"]
    assert dag._hold_reasons(cd, 2, 1) == ([], [])


def test_쿨다운과_alias_변경_둘_다_걸리면_시간_보류_둘(dag):
    _, soft = dag._hold_reasons(
        cooldown(last_drift_age_h=1, last_alias_change="x@v1", last_alias_change_age_h=0.5), 2, 1)
    assert len(soft) == 2


# ---------- decide: Recall 조건 ----------

def test_Recall이_기준_미만이고_표본_충분하면_트리거(dag):
    assert run_decide(dag, recall(0.59, 100), cooldown()) is True


def test_Recall이_기준과_같으면_트리거_안_함(dag):
    # recall < threshold (같으면 통과)
    assert run_decide(dag, recall(0.60, 100), cooldown()) is False


def test_Recall이_기준_이상이면_트리거_안_함(dag):
    assert run_decide(dag, recall(0.82, 500), cooldown()) is False


def test_표본이_최소값과_같으면_판정한다(dag):
    # samples >= min_samples
    assert run_decide(dag, recall(0.30, 100), cooldown()) is True


def test_표본이_최소값_미만이면_Recall이_낮아도_보류(dag):
    assert run_decide(dag, recall(0.30, 99.99), cooldown()) is False


def test_recall이_None이면_보류(dag):
    assert run_decide(dag, recall(None, 0), cooldown()) is False


def test_표본이_충분해도_recall이_None이면_보류(dag):
    assert run_decide(dag, recall(None, 500), cooldown()) is False


def test_실행_파라미터의_기준값을_따른다(dag):
    rec = recall(0.70, 100)
    assert run_decide(dag, rec, cooldown()) is False                         # 기본 0.60: 통과
    assert run_decide(dag, rec, cooldown(), recall_threshold=0.75) is True    # 0.75: 트리거
    assert run_decide(dag, rec, cooldown(), min_samples=200) is False         # 표본 200 필요: 보류


# ---------- decide: PSI ----------

@pytest.mark.parametrize("level", ["stable", "warn", "alert"])
def test_PSI는_어떤_수준이어도_Recall이_정상이면_트리거_안_함(dag, level):
    assert run_decide(dag, recall(0.82, 500), cooldown(), psi_result=psi(level)) is False


def test_PSI_경보여도_Recall_부족이_아니면_로그만_남기고_트리거_안_함(dag, capsys):
    assert run_decide(dag, recall(0.82, 500), cooldown(), psi_result=psi("alert")) is False
    assert "[psi-alert]" in capsys.readouterr().out


def test_PSI가_정상이어도_Recall_부족이면_트리거(dag):
    assert run_decide(dag, recall(0.40, 200), cooldown(), psi_result=psi("stable")) is True


# ---------- decide: 보류 조건과 Recall 조건의 결합 ----------

def test_Recall_부족이어도_고정_보류면_트리거_안_함(dag):
    low = recall(0.40, 200)
    assert run_decide(dag, low, cooldown(serving_version="16")) is False         # 서빙 불일치
    assert run_decide(dag, low, cooldown(serving_version=None)) is False         # 서빙 조회 실패
    assert run_decide(dag, low, cooldown(champion_version=None)) is False        # champion 조회 실패
    assert run_decide(dag, low, cooldown(challenger_version="18")) is False      # 챌린저 검증 중


def test_Recall_부족이어도_시간_보류면_트리거_안_함(dag):
    low = recall(0.40, 200)
    assert run_decide(dag, low, cooldown(last_drift_age_h=1)) is False
    assert run_decide(dag, low, cooldown(last_alias_change="x@v1", last_alias_change_age_h=0.5)) is False


def test_alias_변경_보류는_실행_파라미터의_window를_쓴다(dag):
    low, cd = recall(0.40, 200), cooldown(last_alias_change="x@v1", last_alias_change_age_h=0.5)
    assert run_decide(dag, low, cd, window="1h") is False   # 0.5h < 1h → 보류
    assert run_decide(dag, low, cd, window="30m") is True   # 0.5h < 0.5h 아님 → 보류 해제


def test_쿨다운_경과_후에는_Recall_부족이면_트리거(dag):
    assert run_decide(dag, recall(0.40, 200), cooldown(last_drift_age_h=2.0)) is True


def test_Recall_정상이면_보류_상태여도_트리거_안_함(dag, capsys):
    assert run_decide(dag, recall(0.82, 500), cooldown(last_drift_age_h=1)) is False
    assert "[cooldown-state]" in capsys.readouterr().out


# ---------- decide: force_trigger ----------

def test_force_trigger는_Recall_조건을_우회한다(dag):
    assert run_decide(dag, recall(0.82, 500), cooldown(), force_trigger=True) is True


def test_force_trigger는_표본_부족도_우회한다(dag):
    assert run_decide(dag, recall(None, 0), cooldown(), force_trigger=True) is True


def test_force_trigger는_시간_보류_쿨다운을_우회한다(dag):
    assert run_decide(dag, recall(0.82, 500), cooldown(last_drift_age_h=0.1), force_trigger=True) is True


def test_force_trigger는_시간_보류_alias_변경_직후를_우회한다(dag):
    cd = cooldown(last_alias_change="x@v1", last_alias_change_age_h=0.1)
    assert run_decide(dag, recall(0.82, 500), cd, force_trigger=True) is True


@pytest.mark.parametrize("hard_override", [
    {"serving_version": "16"},       # 서빙 반영 대기
    {"serving_version": None},       # 서빙 조회 실패
    {"champion_version": None},      # champion 조회 실패
    {"challenger_version": "18"},    # 챌린저 검증 중 (10/14 사고 조건)
])
def test_force_trigger도_고정_보류는_우회하지_못한다(dag, hard_override, capsys):
    assert run_decide(dag, recall(0.40, 200), cooldown(**hard_override), force_trigger=True) is False
    assert "force_trigger=True 이지만 고정 보류" in capsys.readouterr().out


def test_force_trigger와_고정_보류_시간_보류가_함께면_트리거_안_함(dag):
    cd = cooldown(challenger_version="18", last_drift_age_h=0.1)
    assert run_decide(dag, recall(0.40, 200), cd, force_trigger=True) is False


# ---------- decide: 실행 파라미터 경고 로그 ----------

def test_기본값과_다른_파라미터는_warn_로그(dag, capsys):
    run_decide(dag, recall(0.82, 500), cooldown(), cooldown_hours=0)
    out = capsys.readouterr().out
    assert "기본값과 다른 실행 파라미터" in out and "cooldown_hours" in out


def test_기본값_그대로면_warn_로그_없음(dag, capsys):
    run_decide(dag, recall(0.82, 500), cooldown())
    assert "기본값과 다른 실행 파라미터" not in capsys.readouterr().out


# ---------- 기본 파라미터가 문서 값과 같은지 ----------

def test_기본_파라미터(dag):
    assert dag.DEFAULT_PARAMS["recall_threshold"] == 0.60
    assert dag.DEFAULT_PARAMS["min_samples"] == 100
    assert dag.DEFAULT_PARAMS["window"] == "1h"
    assert dag.DEFAULT_PARAMS["cooldown_hours"] == 2
    assert dag.DEFAULT_PARAMS["force_trigger"] is False
