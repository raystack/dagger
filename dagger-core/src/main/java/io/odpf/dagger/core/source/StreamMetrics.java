package io.odpf.dagger.core.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_STREAM;
import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_TOPIC;

public class StreamMetrics {
    public static Map<String, List<String>> addDefaultMetrics(StreamConfig streamConfig, Map<String, List<String>> existingMetrics) {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        for (SourceDetails sourceDetails : sourceDetailsArray) {
            SourceName sourceName = sourceDetails.getSourceName();
            switch (sourceName) {
                case KAFKA: {
                    addKafkaDefaultMetrics(streamConfig, existingMetrics);
                    break;
                }
                case PARQUET: {
                    addParquetDefaultMetrics(existingMetrics);
                    break;
                }
                default: {
                    String message = String.format("Error: Cannot add default metrics for unknown source %s", sourceName);
                    throw new IllegalArgumentException(message);
                }
            }
        }
        return existingMetrics;
    }

    public static void addMetric(Map<String, List<String>> metrics, String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    private static void addParquetDefaultMetrics(Map<String, List<String>> ignoredExistingMetrics) {

    }

    private static void addKafkaDefaultMetrics(StreamConfig streamConfig, Map<String, List<String>> existingMetrics) {
        String topicPattern = streamConfig.getKafkaTopicNames();
        List<String> topicsToReport = new ArrayList<>();
        topicsToReport.addAll(Arrays.asList(topicPattern.split("\\|")));
        topicsToReport.forEach(topic -> addMetric(existingMetrics, INPUT_TOPIC.getValue(), topic));

        String streamName = streamConfig.getKafkaName();
        addMetric(existingMetrics, INPUT_STREAM.getValue(), streamName);
    }
}