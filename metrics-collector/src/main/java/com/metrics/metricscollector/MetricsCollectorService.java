package com.metrics.metricscollector;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Map;

@Service
public class MetricsCollectorService {

    private final RestClient restClient;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public MetricsCollectorService(
            KafkaTemplate<String, String> kafkaTemplate) {

        this.restClient = RestClient.builder().build();
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 5000)
    public void collectMetrics() {

        try {
            String metrics = restClient
                    .get()
                    .uri("http://localhost:8080/metrics")
                    .retrieve()
                    .body(String.class);

            if (metrics == null) {
                System.out.println("No metrics received.");
                return;
            }

            kafkaTemplate.send("metrics", "server-01", metrics);

            System.out.println("Published metrics: " + metrics);

        } catch (Exception e) {
            System.err.println(
                    "Failed to collect metrics: " + e.getMessage()
            );
        }
    }
}