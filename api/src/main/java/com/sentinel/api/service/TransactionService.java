package com.sentinel.api.service;

import com.sentinel.domain.entity.Transaction;
import com.sentinel.domain.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);
    private final TransactionRepository transactionRepository;

    public TransactionService(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    @Transactional
    public void save(Transaction transaction) {
        transactionRepository.save(transaction);
        log.info("Transaction saved | transactionId={}", transaction.getTransactionId());
    }
}