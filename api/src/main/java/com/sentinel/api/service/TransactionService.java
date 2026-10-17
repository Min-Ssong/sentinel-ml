package com.sentinel.api.service;

import com.sentinel.domain.entity.FraudResults;
import com.sentinel.domain.entity.ModelRole;
import com.sentinel.domain.entity.Transaction;
import com.sentinel.domain.repository.FraudResultsRepository;
import com.sentinel.domain.repository.ModelMetadataRepository;
import com.sentinel.domain.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);
    private final TransactionRepository transactionRepository;
    private final FraudResultsRepository fraudResultsRepository;
    private final ModelMetadataRepository modelMetadataRepository;

    public TransactionService(TransactionRepository transactionRepository,
                              FraudResultsRepository fraudResultsRepository,
                              ModelMetadataRepository modelMetadataRepository) {
        this.transactionRepository = transactionRepository;
        this.fraudResultsRepository = fraudResultsRepository;
        this.modelMetadataRepository = modelMetadataRepository;
    }

    /** 룰 차단·Fallback 등 모델 판단 결과가 없는 거래 */
    @Transactional
    public void save(Transaction transaction) {
        transactionRepository.save(transaction);
        log.info("Transaction saved | transactionId={}", transaction.getTransactionId());
    }

    /**
     * 거래 + 챔피언 판단 결과를 한 트랜잭션으로 저장 (DB4).
     * 거래만 저장되고 결과가 빠지는 상태가 생기지 않는다.
     */
    @Transactional
    public void saveWithChampionResult(Transaction transaction, long modelId,
                                       double mlScore, String finalDecision) {
        // ID 를 직접 지정하는 엔티티라 save() 는 merge 로 동작 → 반환된 관리 대상 인스턴스를 참조에 사용
        Transaction saved = transactionRepository.save(transaction);
        fraudResultsRepository.save(FraudResults.of(
                saved, modelMetadataRepository.getReferenceById(modelId),
                ModelRole.CHAMPION, mlScore, finalDecision));
        log.info("Transaction saved with champion result | transactionId={} | modelId={}",
                saved.getTransactionId(), modelId);
    }

    /** Shadow 결과 저장. 거래 행이 이미 있어야 한다 (호출 전 exists 로 확인, DB1) */
    @Transactional
    public void saveChallengerResult(String transactionId, long modelId,
                                     double mlScore, String finalDecision) {
        fraudResultsRepository.save(FraudResults.of(
                transactionRepository.getReferenceById(transactionId),
                modelMetadataRepository.getReferenceById(modelId),
                ModelRole.CHALLENGER, mlScore, finalDecision));
    }

    public boolean exists(String transactionId) {
        return transactionRepository.existsById(transactionId);
    }
}
