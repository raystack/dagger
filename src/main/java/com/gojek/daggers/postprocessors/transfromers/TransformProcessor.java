package com.gojek.daggers.postprocessors.transfromers;

import org.apache.flink.configuration.Configuration;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.dagger.transformer.Transformer;
import com.gojek.daggers.exception.TransformClassNotDefinedException;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.metrics.telemetry.TelemetryTypes;
import com.gojek.daggers.postprocessors.PostProcessorConfig;
import com.gojek.daggers.postprocessors.common.PostProcessor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.TRANSFORM_PROCESSOR;

public class TransformProcessor implements PostProcessor, TelemetryPublisher {
    private List<TransformConfig> transformConfigs;
    private Configuration configuration;
    private Map<String, List<String>> metrics = new HashMap<>();

    public TransformProcessor(List<TransformConfig> transformConfigs, Configuration configuration) {
        this.transformConfigs = transformConfigs;
        this.configuration = configuration;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        for (TransformConfig transformConfig : transformConfigs) {
            transformConfig.validateFields();
            String className = transformConfig.getTransformationClass();
            try {
                Transformer function = getTransformMethod(transformConfig, className, streamInfo.getColumnNames());
                streamInfo = function.transform(streamInfo);
            } catch (ReflectiveOperationException e) {
                throw new TransformClassNotDefinedException(e.getMessage());
            }
        }
        return streamInfo;
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return postProcessorConfig.hasTransformConfigs();
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(TelemetryTypes.POST_PROCESSOR_TYPE.getValue(), TRANSFORM_PROCESSOR);
    }

    protected Transformer getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        Class<?> transformerClass = Class.forName(className);
        Constructor transformerClassConstructor = transformerClass.getConstructor(Map.class, String[].class, Configuration.class);
        return (Transformer) transformerClassConstructor.newInstance(transformConfig.getTransformationArguments(), columnNames, configuration);
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
