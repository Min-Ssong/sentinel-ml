package com.sentinelml.simulator.loader;

import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Component
public class CreditCardFeatureLoader {

    private static final int FEATURE_COUNT = 30;        // Time, V1~V28, Amount
    private static final int CLASS_INDEX = 30;          // 31번째 컬럼
    private static final int NORMAL_POOL_CAP = 50000;   // Class=0 메모리 상한

    private final List<List<Double>> fraudFeatures = new ArrayList<>();   // Class=1
    private final List<List<Double>> normalFeatures = new ArrayList<>();  // Class=0
    private final Random random = new Random();

    public void load(String csvPath) {
        long total = countDataRows(csvPath);
        long skip = (long) (total * 0.8);       // 앞 80%는 champion 학습 구간 → 건너뜀
        try (BufferedReader reader = Files.newBufferedReader(Path.of(csvPath))) {
            reader.readLine();  // 헤더 스킵
            String line;
            long rowIdx = 0;
            while ((line = reader.readLine()) != null) {
                if (rowIdx++ < skip) continue;
                String[] tokens = line.split(",");
                int label = Integer.parseInt(unquote(tokens[CLASS_INDEX]));
                if (label == 1) {
                    fraudFeatures.add(parseFeatures(tokens));
                } else {
                    normalFeatures.add(parseFeatures(tokens));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("creditcard.csv 로드 실패: " + csvPath, e);
        }
        if (fraudFeatures.isEmpty() || normalFeatures.isEmpty()) {
            throw new IllegalStateException("피처 풀이 비었습니다. fraud="
                    + fraudFeatures.size() + ", normal=" + normalFeatures.size());
        }
        System.out.printf("CSV loaded | fraud=%d | normal=%d%n",
                fraudFeatures.size(), normalFeatures.size());
    }

    private long countDataRows(String csvPath) {
        try (var lines = Files.lines(Path.of(csvPath))) {
            return lines.count() - 1;           // 헤더 제외
        } catch (IOException e) {
            throw new UncheckedIOException("creditcard.csv 행 수 계산 실패: " + csvPath, e);
        }
    }

    public List<Double> randomFraud() {
        return fraudFeatures.get(random.nextInt(fraudFeatures.size()));
    }

    public List<Double> randomNormal() {
        return normalFeatures.get(random.nextInt(normalFeatures.size()));
    }

    private List<Double> parseFeatures(String[] tokens) {
        List<Double> features = new ArrayList<>(FEATURE_COUNT);
        for (int i = 0; i < FEATURE_COUNT; i++) {
            features.add(Double.parseDouble(unquote(tokens[i])));
        }
        return features;
    }

    private String unquote(String s) {
        return s.replace("\"", "").trim();
    }
}