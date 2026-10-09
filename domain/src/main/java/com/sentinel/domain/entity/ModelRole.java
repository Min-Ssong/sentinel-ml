package com.sentinel.domain.entity;

/** FRAUD_RESULTS.ROLE — 같은 모델 버전도 시점에 따라 챌린저였다가 챔피언이 되므로 행마다 기록한다 */
public enum ModelRole {
    CHAMPION,    // 실제 판정에 쓰인 결과
    CHALLENGER   // Shadow 병행 추론 결과 (판정에 쓰이지 않음)
}
