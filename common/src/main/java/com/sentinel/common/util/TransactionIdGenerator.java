package com.sentinel.common.util;

import java.util.UUID;

public class TransactionIdGenerator {

    public static String generate() {
        return UUID.randomUUID().toString();
    }

    public static boolean isValid(String txId) {
        if (txId == null || txId.isBlank()) return false;
        try {
            UUID.fromString(txId);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}