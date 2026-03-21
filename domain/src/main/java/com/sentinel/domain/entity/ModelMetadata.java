package com.sentinel.domain.entity;

import jakarta.persistence.*;
        import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Table(name = "model_metadata")
@EntityListeners(AuditingEntityListener.class)
public class ModelMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 100)
    private String modelName;

    @Column(nullable = false, length = 20)
    private String version;

    @Column(nullable = false, length = 36)
    private String mlflowRunId;

    @Column(nullable = false)
    private boolean isActive;

    @Column(name = "is_active", nullable = false)
    private boolean active;

    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}