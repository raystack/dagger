package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.exception.TransformClassNotDefinedException;
import com.gojek.daggers.postprocessor.parser.TransformConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

public class TransformProcessor implements PostProcessor {
    private List<TransformConfig> transformConfigs;

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

    protected MapFunction<Row, Row> getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        Class<?> transformerClass = Class.forName(className);
        Constructor transformerClassConstructor = transformerClass.getConstructor(Map.class, String[].class);
        return (MapFunction<Row, Row>) transformerClassConstructor.newInstance(transformConfig.getTransformationArguments(), columnNames);
    }
}
