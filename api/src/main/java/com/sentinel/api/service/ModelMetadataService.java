package com.sentinel.api.service;

import com.sentinel.domain.entity.ModelMetadata;
import com.sentinel.domain.repository.ModelMetadataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 추론 서버 응답의 (모델 이름, 버전, run_id)로 MODEL_METADATA 행을 찾거나 만든다 (DB2).
 *
 * - 응답마다 실제로 점수를 낸 모델이 오므로, 서빙만 재기동해도 올바른 버전이 기록된다.
 * - 버전별 ID 는 바뀌지 않으므로 메모리에 캐시한다 (조회는 버전당 최초 1회).
 * - 챔피언·Shadow 컨슈머가 같은 버전을 동시에 처음 만나면 UNIQUE(MODEL_NAME, VERSION) 위반이 날 수 있어,
 *   그때는 다시 조회한다.
 * - 호출부(컨슈머)가 트랜잭션 밖이어야 한다. 리포지토리 저장이 자체 트랜잭션으로 커밋되고,
 *   유일성 위반이 바깥 트랜잭션을 rollback-only 로 만들지 않게 하기 위함.
 */
@Service
public class ModelMetadataService {

    private static final Logger log = LoggerFactory.getLogger(ModelMetadataService.class);

    private final ModelMetadataRepository repository;
    private final Map<String, Long> idCache = new ConcurrentHashMap<>();

    public ModelMetadataService(ModelMetadataRepository repository) {
        this.repository = repository;
    }

    /** 모델 정보가 하나라도 없으면 empty (구버전 서버 응답 등) */
    public Optional<Long> resolveId(String modelName, String version, String runId) {
        if (modelName == null || version == null || runId == null) {
            return Optional.empty();
        }
        String key = modelName + ":" + version;
        Long cached = idCache.get(key);
        if (cached != null) {
            return Optional.of(cached);
        }
        ModelMetadata found = repository.findByModelNameAndVersion(modelName, version)
                .orElseGet(() -> create(modelName, version, runId));
        idCache.put(key, found.getId());
        return Optional.of(found.getId());
    }

    private ModelMetadata create(String modelName, String version, String runId) {
        ModelMetadata m = new ModelMetadata();
        m.setModelName(modelName);
        m.setVersion(version);
        m.setMlflowRunId(runId);
        m.setActive(false);   // 역할(챔피언/챌린저)은 FRAUD_RESULTS.ROLE 로 기록 — 여기서는 관리하지 않음
        try {
            ModelMetadata saved = repository.saveAndFlush(m);
            log.info("Model metadata registered | name={} | version={} | runId={} | id={}",
                    modelName, version, runId, saved.getId());
            return saved;
        } catch (DataIntegrityViolationException e) {
            // 다른 컨슈머가 먼저 등록함 → 그 행을 사용
            return repository.findByModelNameAndVersion(modelName, version)
                    .orElseThrow(() -> e);
        }
    }
}
