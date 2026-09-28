package com.metrics.metricsprocessor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class MetricsKafkaConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final InfluxDBClient influxDBClient;

    private final WriteApiBlocking writeApi;

    public MetricsKafkaConsumer(
            @Value("${influxdb.url}") String url,
            @Value("${influxdb.token}") String token,
            @Value("${influxdb.org}") String org,
            @Value("${influxdb.bucket}") String bucket) {

        this.influxDBClient = InfluxDBClientFactory.create(
                url,
                token.toCharArray(),
                org,
                bucket
        );

        this.writeApi = influxDBClient.getWriteApiBlocking();
    }

    @KafkaListener(
            topics = "metrics",
            groupId = "metrics-processor-group"
    )
    public void consume(String message) {

        try {
            System.out.println(
                    "Received metrics from Kafka: " + message
            );

            JsonNode json = objectMapper.readTree(message);

            String server = json.get("server").asText();

            double memory = json.get("memory").asDouble();

            double cpu = json.get("cpu").asDouble();

            double requestsPerSecond =
                    json.get("requestsPerSecond").asDouble();

            double errorRate =
                    json.get("errorRate").asDouble();

            Point point = Point
                    .measurement("server_metrics")
                    .addTag("server", server)
                    .addField("memory", memory)
                    .addField("cpu", cpu)
                    .addField(
                            "requestsPerSecond",
                            requestsPerSecond
                    )
                    .addField("errorRate", errorRate)
                    .time(
                            Instant.now(),
                            WritePrecision.NS
                    );

            writeApi.writePoint(point);

            System.out.println(
                    "Written metrics to InfluxDB: " + point
            );

        } catch (Exception e) {

            System.err.println(
                    "Failed to process metrics: "
                            + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}