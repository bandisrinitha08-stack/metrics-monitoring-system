package com.metrics.demoserver;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class MetricsController {

    private final Random random = new Random();

    @GetMapping("/metrics")
    public Map<String, Object> getMetrics() {

        Map<String, Object> metrics = new HashMap<>();

        metrics.put("server", "server-01");
        metrics.put("cpu", Math.round(random.nextDouble() * 1000.0) / 10.0);
        metrics.put("memory", Math.round(random.nextDouble() * 1000.0) / 10.0);
        metrics.put("requestsPerSecond", random.nextInt(200));
        metrics.put("errorRate", Math.round(random.nextDouble() * 50.0) / 10.0);

        return metrics;
    }
}