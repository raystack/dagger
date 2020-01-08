package com.gojek.daggers.postProcessors.transfromers;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.exception.TransformClassNotDefinedException;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.metrics.telemetry.TelemetryTypes;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.TRANSFORM_PROCESSOR;

public class TransformProcessor implements PostProcessor, TelemetryPublisher {
    private List<TransformConfig> transformConfigs;
    private Map<String, List<String>> metrics = new HashMap<>();

    public TransformProcessor(List<TransformConfig> transformConfigs) {
        this.transformConfigs = transformConfigs;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        DataStream<Row> resultStream = streamInfo.getDataStream();
        for (TransformConfig transformConfig : transformConfigs) {
            transformConfig.validateFields();
            String className = transformConfig.getTransformationClass();
            try {
                MapFunction<Row, Row> mapFunction = getTransformMethod(transformConfig, className, streamInfo.getColumnNames());
                resultStream = streamInfo.getDataStream().map(mapFunction);
            } catch (ReflectiveOperationException e) {
                throw new TransformClassNotDefinedException(e.getMessage());
            }
        }
        return new StreamInfo(resultStream, streamInfo.getColumnNames());
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return postProcessorConfig.hasTransformConfigs();
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(TelemetryTypes.POST_PROCESSOR_TYPE.getValue(), TRANSFORM_PROCESSOR);
    }

    protected MapFunction<Row, Row> getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        Class<?> transformerClass = Class.forName(className);
        Constructor transformerClassConstructor = transformerClass.getConstructor(Map.class, String[].class);
        return (MapFunction<Row, Row>) transformerClassConstructor.newInstance(transformConfig.getTransformationArguments(), columnNames);
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
