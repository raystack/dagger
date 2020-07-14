package com.gojek.daggers.sink;

import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FlinkKafkaProducer010Custom<T> extends RichSinkFunction<T> implements CheckpointedFunction, CheckpointListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKafkaProducer010Custom.class.getName());
    private Configuration configuration;
    private ErrorReporter errorReporter;
    private FlinkKafkaProducer<T> flinkKafkaProducer;

    public FlinkKafkaProducer010Custom(String topicId, KafkaSerializationSchema<T> serializationSchema,
                                       Properties producerConfig, Configuration configuration) {
        this.flinkKafkaProducer = new FlinkKafkaProducer<>(topicId, serializationSchema, producerConfig, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        this.configuration = configuration;
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return this.flinkKafkaProducer.getIterationRuntimeContext();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return this.flinkKafkaProducer.getRuntimeContext();
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        this.flinkKafkaProducer.setRuntimeContext(t);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.flinkKafkaProducer.open(parameters);
    }

    @Override
    public void close() throws Exception {
        this.flinkKafkaProducer.close();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        try {
            flinkKafkaProducer.invoke(value, context);
            LOGGER.info("row to kafka :" + value.toString());
        } catch (Exception exception) {
            errorReporter = getErrorReporter(getRuntimeContext());
            errorReporter.reportFatalException(exception);
            throw exception;
        }
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        this.flinkKafkaProducer.notifyCheckpointComplete(l);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        this.flinkKafkaProducer.snapshotState(functionSnapshotContext);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        this.flinkKafkaProducer.initializeState(functionInitializationContext);
    }

    protected ErrorReporter getErrorReporter(RuntimeContext runtimeContext) {
        return ErrorReporterFactory.getErrorReporter(runtimeContext, configuration);
    }
}
