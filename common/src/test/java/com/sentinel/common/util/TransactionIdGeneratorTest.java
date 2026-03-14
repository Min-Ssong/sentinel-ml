package com.sentinel.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TransactionIdGeneratorTest {

    @Test
    @DisplayName("생성된 txId는 UUID 형식이어야 한다")
    void generate_shouldReturnValidUUID() {
        // given & when
        String txId = TransactionIdGenerator.generate();

        // then
        assertTrue(TransactionIdGenerator.isValid(txId));
    }

    @Test
    @DisplayName("null은 유효하지 않은 txId다")
    void isValid_shouldReturnFalse_whenNull() {
        // given & when & then
        assertFalse(TransactionIdGenerator.isValid(null));
    }

    @Test
    @DisplayName("빈 문자열은 유효하지 않은 txId다")
    void isValid_shouldReturnFalse_whenBlank() {
        // given & when & then
        assertFalse(TransactionIdGenerator.isValid(""));
    }

    @Test
    @DisplayName("UUID 형식이 아닌 문자열은 유효하지 않은 txId다")
    void isValid_shouldReturnFalse_whenNotUUIDFormat() {
        // given & when & then
        assertFalse(TransactionIdGenerator.isValid("invalid-tx-id"));
    }
}