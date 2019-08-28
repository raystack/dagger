package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.async.decorator.StreamDecorator;
import com.gojek.daggers.async.decorator.StreamDecoratorFactory;
import com.gojek.de.stencil.StencilClient;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.Map;

import static com.gojek.daggers.Constants.*;

public class AshikoProcessor implements PostProcessor {

    private Configuration configuration;
    private StencilClient stencilClient;

    public AshikoProcessor(Configuration configuration, StencilClient stencilClient) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        String asyncConfigurationString = configuration.getString(ASYNC_IO_KEY, "");
        Map<String, Object> asyncConfig = new Gson().fromJson(asyncConfigurationString, Map.class);
        Descriptors.Descriptor outputDescriptor = this.outputDescriptor();
        int size = outputDescriptor.getFields().size();
        String[] columnNames = new String[size];
        DataStream<Row> resultStream = streamInfo.getDataStream();
        for (Descriptors.FieldDescriptor fieldDescriptor : outputDescriptor.getFields()) {
            String fieldName = fieldDescriptor.getName();
            if (!asyncConfig.containsKey(fieldName)) {
                continue;
            }
            Map<String, String> fieldConfiguration = ((Map<String, String>) asyncConfig.get(fieldName));
            int asyncIOCapacity = Integer.valueOf(fieldConfiguration.getOrDefault(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT));
            int fieldIndex = fieldDescriptor.getIndex();
            fieldConfiguration.put(FIELD_NAME_KEY, fieldName);
            StreamDecorator streamDecorator = StreamDecoratorFactory.getStreamDecorator(fieldConfiguration, fieldIndex, stencilClient, asyncIOCapacity, size);
            columnNames[fieldIndex] = fieldName;
            resultStream = streamDecorator.decorate(resultStream);
        }
        return new StreamInfo(resultStream, columnNames);
    }

    // TODO: Remove this switch when migration to new portal is done
    private Descriptors.Descriptor outputDescriptor() {
        // Move conteont inside this block to process method
        if (configuration.getString(PORTAL_VERSION, "1") == "2") {
            String protoClassName = configuration.getString(OUTPUT_PROTO_MESSAGE, "");
            return stencilClient.get(protoClassName);
        }

        String outputProtoPrefix = configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "");
        String protoClassName = String.format("%sMessage", outputProtoPrefix);
        return stencilClient.get(protoClassName);
    }
}
