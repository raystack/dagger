package com.gojek.daggers.sink;

import com.gojek.daggers.metrics.ErrorStatsReporter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Properties;

import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_VALUE_DEFAULT;

public class FlinkKafkaProducer010Custom<T> extends FlinkKafkaProducer010<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKafkaProducer010Custom.class.getName());
    private Configuration configuration;
    private ErrorStatsReporter errorStatsReporter;

    public FlinkKafkaProducer010Custom(String topicId, KeyedSerializationSchema<T> serializationSchema,
                                       Properties producerConfig, @Nullable FlinkKafkaPartitioner<T> customPartitioner, Configuration configuration) {
        super(topicId, serializationSchema, producerConfig, customPartitioner);
        this.configuration = configuration;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        try {
            invokeBaseProducer(value, context);
            LOGGER.info("row to kafka :" + value.toString());
        } catch (Exception exception) {
            if (configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)) {
                errorStatsReporter = getErrorStatsReporter(getRuntimeContext());
                errorStatsReporter.reportFatalException(exception);
            }
            throw exception;
        }
    }

    protected void invokeBaseProducer(T value, Context context) throws Exception {
        super.invoke(value, context);
    }

    protected ErrorStatsReporter getErrorStatsReporter(RuntimeContext runtimeContext) {
        return new ErrorStatsReporter(runtimeContext, configuration);
    }
}
