package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
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
            String className = transformConfig.getTransformationClass();
            try {
                MapFunction<Row, Row> mapFunction = getTransformMethod(transformConfig, className);
                resultStream = streamInfo.getDataStream().map(mapFunction);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return new StreamInfo(resultStream, streamInfo.getColumnNames());
    }

    protected MapFunction<Row, Row> getTransformMethod(TransformConfig transformConfig, String className) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        Class<?> transformerClass = Class.forName(className);
        Constructor transformerClassConstructor = transformerClass.getConstructor(Map.class);
        return (MapFunction<Row, Row>) transformerClassConstructor.newInstance(transformConfig.getTransformationArguments());
    }
}
