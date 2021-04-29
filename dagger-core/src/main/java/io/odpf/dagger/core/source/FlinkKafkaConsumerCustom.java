package io.odpf.dagger.core.source;

import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.regex.Pattern;

public class FlinkKafkaConsumerCustom extends FlinkKafkaConsumer<Row> {

    private Configuration configuration;
    private ErrorReporter errorReporter;

    public FlinkKafkaConsumerCustom(Pattern subscriptionPattern, KafkaDeserializationSchema<Row> deserializer,
                                    Properties props, Configuration configuration) {
        super(subscriptionPattern, deserializer, props);
        this.configuration = configuration;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        try {
            runBaseConsumer(sourceContext);
        } catch (ExceptionInChainedOperatorException chainedOperatorException) {
            throw chainedOperatorException;
        } catch (Exception exception) {
            errorReporter = getErrorReporter(getRuntimeContext());
            errorReporter.reportFatalException(exception);
            throw exception;
        }
    }

    protected void runBaseConsumer(SourceContext<Row> sourceContext) throws Exception {
        super.run(sourceContext);
    }

    protected ErrorReporter getErrorReporter(RuntimeContext runtimeContext) {
        return ErrorReporterFactory.getErrorReporter(runtimeContext, configuration);
    }
}
