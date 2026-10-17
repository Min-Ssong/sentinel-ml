package com.sentinel.domain.repository;

import com.sentinel.domain.entity.FraudResults;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FraudResultsRepository extends JpaRepository<FraudResults, Long> {
}
