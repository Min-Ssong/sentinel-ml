package com.sentinel.domain.repository;

import com.sentinel.domain.entity.ModelMetadata;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ModelMetadataRepository extends JpaRepository<ModelMetadata, Long> {

    Optional<ModelMetadata> findByModelNameAndVersion(String modelName, String version);
}
