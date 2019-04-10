package com.gojek.daggers;

import com.gojek.daggers.decorator.StreamDecorator;
import com.gojek.daggers.decorator.StreamDecoratorFactory;
import com.gojek.de.stencil.StencilClient;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.Map;

import static com.gojek.daggers.Constants.*;

public class DeNormaliseStream {
    private DataStream<Row> dataStream;
    private Configuration configuration;
    private Table table;
    private StencilClient stencilClient;

    public DeNormaliseStream(DataStream<Row> dataStream, Configuration configuration, Table table, StencilClient stencilClient) {
        this.dataStream = dataStream;
        this.configuration = configuration;
        this.table = table;
        this.stencilClient = stencilClient;
    }

    public void apply() {
        if (!configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)) {
            dataStream.addSink(SinkFactory.getSinkFunction(configuration, table.getSchema().getColumnNames(), stencilClient));
            return;
        }
        deNormaliseUsingEs();
    }

    private void deNormaliseUsingEs() {
        String asyncConfigurationString = configuration.getString(ASYNC_IO_KEY, "");
        Map<String, Object> asyncConfig = new Gson().fromJson(asyncConfigurationString, Map.class);
        String outputProtoPrefix = configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "");
        Descriptor outputDescriptor = stencilClient.get(String.format("%sMessage", outputProtoPrefix));
        int size = outputDescriptor.getFields().size();
        String[] columnNames = new String[size];
        org.apache.flink.streaming.api.datastream.DataStream<Row> resultStream = dataStream.javaStream();
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
        resultStream.addSink(SinkFactory.getSinkFunction(configuration, columnNames, stencilClient));
    }
}
