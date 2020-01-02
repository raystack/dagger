package com.gojek.daggers.source;

import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;

import java.util.Properties;
import java.util.regex.Pattern;

public class FlinkKafkaConsumer011Custom<T> extends FlinkKafkaConsumer011<T> {

    private Configuration configuration;
    private ErrorReporter errorReporter;

    public FlinkKafkaConsumer011Custom(Pattern subscriptionPattern, KafkaDeserializationSchema<T> deserializer,
                                       Properties props, Configuration configuration) {
        super(subscriptionPattern, deserializer, props);
        this.configuration = configuration;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
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

    protected void runBaseConsumer(SourceContext<T> sourceContext) throws Exception {
        super.run(sourceContext);
    }

    protected ErrorReporter getErrorReporter(RuntimeContext runtimeContext) {
        return ErrorReporterFactory.getErrorReporter(runtimeContext, configuration);
    }
}
