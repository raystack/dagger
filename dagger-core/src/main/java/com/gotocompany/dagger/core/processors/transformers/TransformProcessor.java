package com.gotocompany.dagger.core.processors.transformers;

import com.gotocompany.dagger.common.core.DaggerContext;
import com.gotocompany.dagger.common.core.StreamInfo;
import com.gotocompany.dagger.common.core.Transformer;
import com.gotocompany.dagger.core.exception.TransformClassNotDefinedException;
import com.gotocompany.dagger.core.metrics.telemetry.TelemetryPublisher;
import com.gotocompany.dagger.core.metrics.telemetry.TelemetryTypes;
import com.gotocompany.dagger.core.processors.PostProcessorConfig;
import com.gotocompany.dagger.core.processors.PreProcessorConfig;
import com.gotocompany.dagger.core.processors.types.PostProcessor;
import com.gotocompany.dagger.core.processors.types.Preprocessor;
import com.gotocompany.dagger.core.utils.Constants;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Transformer processor.
 */
public class TransformProcessor implements Preprocessor, PostProcessor, TelemetryPublisher {
    protected final List<TransformConfig> transformConfigs;


    /**
     * Gets table name.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    protected final String tableName;
    private final Map<String, List<String>> metrics = new HashMap<>();
    protected final TelemetryTypes type;
    private final DaggerContext daggerContext;

    /**
     * Instantiates a new Transform processor.
     *
     * @param transformConfigs the transform configs
     * @param daggerContext    the daggerContext
     */
    public TransformProcessor(List<TransformConfig> transformConfigs, DaggerContext daggerContext) {
        this("NULL", TelemetryTypes.POST_PROCESSOR_TYPE, transformConfigs, daggerContext);
    }

    /**
     * Instantiates a new Transform processor with specified table name and telemetry types.
     *
     * @param tableName     the table name
     * @param type          the type
     * @param daggerContext the configuration
     */
    public TransformProcessor(String tableName, TelemetryTypes type, List<TransformConfig> transformConfigs, DaggerContext daggerContext) {
        this.transformConfigs = transformConfigs == null ? new ArrayList<>() : transformConfigs;
        this.tableName = tableName;
        this.type = type;
        this.daggerContext = daggerContext;
        TransformerUtils.populateDefaultArguments(this);
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        for (TransformConfig transformConfig : transformConfigs) {
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
    public boolean canProcess(PreProcessorConfig processorConfig) {
        return processorConfig.getTableTransformers().stream().anyMatch(x -> x.tableName.equals(this.tableName));
    }

    @Override
    public boolean canProcess(PostProcessorConfig processorConfig) {
        return processorConfig.hasTransformConfigs();
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        switch (this.type) {
            case POST_PROCESSOR_TYPE:
                addMetric(type.getValue(), Constants.TRANSFORM_PROCESSOR_KEY);
                break;
            case PRE_PROCESSOR_TYPE:
                addMetric(type.getValue(), this.tableName + "_" + Constants.TRANSFORM_PROCESSOR_KEY);
                break;
            default:
                break;
        }
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    /**
     * Gets transform method.
     *
     * @param transformConfig the transform config
     * @param className       the class name
     * @param columnNames     the column names
     * @return the transform method
     * @throws ClassNotFoundException    the class not found exception
     * @throws NoSuchMethodException     the no such method exception
     * @throws InstantiationException    the instantiation exception
     * @throws IllegalAccessException    the illegal access exception
     * @throws InvocationTargetException the invocation target exception
     */
    protected Transformer getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Class<?> transformerClass = Class.forName(className);
        Constructor transformerClassConstructor = transformerClass.getConstructor(Map.class, String[].class, DaggerContext.class);
        return (Transformer) transformerClassConstructor.newInstance(transformConfig.getTransformationArguments(), columnNames, daggerContext);
    }
}
