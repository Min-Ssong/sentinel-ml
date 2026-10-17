package com.sentinel.domain.repository;

import com.sentinel.domain.entity.TransactionLabel;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransactionLabelRepository extends JpaRepository<TransactionLabel, String> {
}
