package com.gojek.daggers.sink;

import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkKafkaProducer010Custom<T> extends FlinkKafkaProducer010<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKafkaProducer010Custom.class.getName());
    private Configuration configuration;
    private ErrorReporter errorReporter;

    public FlinkKafkaProducer010Custom(String topicId, KeyedSerializationSchema<T> serializationSchema,
                                       Properties producerConfig, @Nullable FlinkKafkaPartitioner<T> customPartitioner, Configuration configuration) {
        super(topicId, serializationSchema, producerConfig, customPartitioner);
        this.configuration = configuration;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        try {
            invokeBaseProducer(value, context);
            LOGGER.info("range to kafka :" + value.toString());
        } catch (Exception exception) {
            errorReporter = getErrorReporter(getRuntimeContext());
            errorReporter.reportFatalException(exception);
            throw exception;
        }
    }

    protected void invokeBaseProducer(T value, Context context) throws Exception {
        super.invoke(value, context);
    }

    protected ErrorReporter getErrorReporter(RuntimeContext runtimeContext) {
        return ErrorReporterFactory.getErrorReporter(runtimeContext, configuration);
    }
}
