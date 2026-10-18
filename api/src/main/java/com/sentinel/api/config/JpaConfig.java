package com.sentinel.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.time.Instant;
import java.util.Optional;

@Configuration
@EnableJpaRepositories(basePackages = "com.sentinel.domain.repository")
public class JpaConfig {

    /**
     * @CreatedDate 값 공급자. 기본 공급자는 LocalDateTime.now() 를 만든 뒤 대상 타입으로 변환하므로
     * JVM 시간대에 의존한다. Instant.now() 를 직접 주어 그 경로를 없앤다 (R2, 2026-10-18).
     * SentinelApplication 의 @EnableJpaAuditing(dateTimeProviderRef = "auditingDateTimeProvider") 와 짝.
     */
    @Bean
    public DateTimeProvider auditingDateTimeProvider() {
        return () -> Optional.of(Instant.now());
    }
}
