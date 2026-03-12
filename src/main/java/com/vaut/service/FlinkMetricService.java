package com.vaut.service;

import com.vaut.entity.FlinkMetric;
import com.vaut.repository.FlinkMetricRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class FlinkMetricService {

    private final FlinkMetricRepository repository;

    @PostConstruct
    public void initExamples() {
        if (repository.count() == 0) {
            repository.save(FlinkMetric.builder()
                    .name("kafka_events_total")
                    .type("COUNTER")
                    .query("SELECT COUNT(*) FROM kafka_source")
                    .labels("app=kafka-consumer,env=prod")
                    .description("Total number of events consumed from Kafka")
                    .status("ACTIVE")
                    .build());

            repository.save(FlinkMetric.builder()
                    .name("avg_processing_latency")
                    .type("GAUGE")
                    .query("SELECT AVG(PROCTIME() - rowtime) FROM kafka_source")
                    .labels("app=kafka-consumer")
                    .description("Average processing latency in milliseconds")
                    .status("ACTIVE")
                    .build());

            repository.save(FlinkMetric.builder()
                    .name("events_per_minute")
                    .type("HISTOGRAM")
                    .query("SELECT window_start, window_end, COUNT(*) FROM TABLE(TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)) GROUP BY window_start, window_end")
                    .labels("app=kafka-consumer,window=1m")
                    .description("Distribution of events per minute window")
                    .status("INITIALIZING")
                    .build());
        }
    }

    public List<FlinkMetric> getAllMetrics() {
        return repository.findAll();
    }

    @Transactional
    public FlinkMetric saveMetric(FlinkMetric metric) {
        return repository.save(metric);
    }

    @Transactional
    public void deleteMetric(Long id) {
        repository.deleteById(id);
    }

    public String getQueryTemplate(String type) {
        return switch (type.toUpperCase()) {
            case "COUNTER" -> "SELECT COUNT(*) FROM your_table";
            case "GAUGE" -> "SELECT AVG(value_field) FROM your_table";
            case "HISTOGRAM" -> "SELECT window_start, window_end, COUNT(*) FROM TABLE(TUMBLE(TABLE your_table, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) GROUP BY window_start, window_end";
            default -> "SELECT * FROM your_table";
        };
    }
}
