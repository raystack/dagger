package com.gotocompany.dagger.core.sink;

import com.gotocompany.dagger.core.metrics.reporters.statsd.DaggerStatsDReporter;
import com.gotocompany.dagger.core.metrics.telemetry.TelemetryPublisher;
import com.gotocompany.dagger.core.metrics.telemetry.TelemetryTypes;
import com.gotocompany.dagger.core.sink.bigquery.BigQuerySinkBuilder;
import com.gotocompany.dagger.core.sink.influx.ErrorHandler;
import com.gotocompany.dagger.core.sink.influx.InfluxDBFactoryWrapper;
import com.gotocompany.dagger.core.sink.influx.InfluxDBSink;
import com.gotocompany.dagger.core.utils.Constants;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.types.Row;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import com.gotocompany.dagger.core.sink.kafka.KafkaSerializationSchemaFactory;
import com.gotocompany.dagger.core.sink.kafka.KafkaSerializerBuilder;
import com.gotocompany.dagger.core.sink.log.LogSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The Sink orchestrator.
 * Responsible for handling the sink type.
 */
public class SinkOrchestrator implements TelemetryPublisher {
    private final MetricsTelemetryExporter telemetryExporter;
    private final Map<String, List<String>> metrics;

    public SinkOrchestrator(MetricsTelemetryExporter telemetryExporter) {
        this.telemetryExporter = telemetryExporter;
        this.metrics = new HashMap<>();
    }

    /**
     * Gets sink.
     *
     * @return the sink
     * @configuration configuration             the configuration
     * @columnNames columnNames               the column names
     * @StencilClientOrchestrator stencilClientOrchestrator the stencil client orchestrator
     */
    public Sink getSink(Configuration configuration, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator, DaggerStatsDReporter daggerStatsDReporter) {
        String sinkType = configuration.getString("SINK_TYPE", "influx");
        addMetric(TelemetryTypes.SINK_TYPE.getValue(), sinkType);
        Sink sink;
        switch (sinkType) {
            case "kafka":
                String outputBootStrapServers = configuration.getString(Constants.SINK_KAFKA_BROKERS_KEY, "");

                KafkaSerializerBuilder serializationSchema = KafkaSerializationSchemaFactory
                        .getSerializationSchema(configuration, stencilClientOrchestrator, columnNames);

                reportTelemetry(serializationSchema);

                sink = KafkaSink.<Row>builder()
                        .setBootstrapServers(outputBootStrapServers)
                        .setKafkaProducerConfig(getProducerProperties(configuration))
                        .setRecordSerializer(serializationSchema.build())
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

                break;
            case "log":
                sink = new LogSink(columnNames);
                break;
            case "bigquery":
                sink = BigQuerySinkBuilder.create()
                        .setColumnNames(columnNames)
                        .setDaggerStatsDReporter(daggerStatsDReporter)
                        .setConfiguration(configuration)
                        .setStencilClientOrchestrator(stencilClientOrchestrator)
                        .build();
                break;
            default:
                sink = new InfluxDBSink(new InfluxDBFactoryWrapper(), configuration, columnNames, new ErrorHandler());
        }
        notifySubscriber();
        return sink;
    }

    private void reportTelemetry(KafkaSerializerBuilder kafkaSchemaBuilder) {
        TelemetryPublisher pub = (TelemetryPublisher) kafkaSchemaBuilder;
        pub.addSubscriber(telemetryExporter);
    }


    /**
     * Gets producer properties.
     *
     * @param configuration the configuration
     * @return the producer properties
     */
    protected Properties getProducerProperties(Configuration configuration) {
        String outputBrokerList = configuration.getString(Constants.SINK_KAFKA_BROKERS_KEY, "");
        Properties kafkaProducerConfigs = FlinkKafkaProducerBase.getPropertiesFromBrokerList(outputBrokerList);
        if (configuration.getBoolean(Constants.SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE_KEY, Constants.SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE_DEFAULT)) {
            kafkaProducerConfigs.setProperty(Constants.SINK_KAFKA_COMPRESSION_TYPE_KEY, Constants.SINK_KAFKA_COMPRESSION_TYPE_DEFAULT);
            kafkaProducerConfigs.setProperty(Constants.SINK_KAFKA_MAX_REQUEST_SIZE_KEY, Constants.SINK_KAFKA_MAX_REQUEST_SIZE_DEFAULT);
        }
        String lingerMs = configuration.getString(Constants.SINK_KAFKA_LINGER_MS_KEY, Constants.SINK_KAFKA_LINGER_MS_DEFAULT);
        validateLingerMs(lingerMs);
        kafkaProducerConfigs.setProperty(Constants.SINK_KAFKA_LINGER_MS_CONFIG_KEY, lingerMs);

        return kafkaProducerConfigs;
    }

    private void validateLingerMs(String lingerMs) {
        try {
            Integer.parseInt(lingerMs);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Provided value for Linger Ms : " + lingerMs+ " is not a valid integer , Error: " + e.getMessage() );
        }
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
