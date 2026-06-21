package com.sentinelml.simulator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "simulator")
public record SimulatorProperties(
        @DefaultValue("10") int rate,
        @DefaultValue("0.1") double fraudRatio,
        @DefaultValue("../ml/data/creditcard.csv") String csvPath
) {}