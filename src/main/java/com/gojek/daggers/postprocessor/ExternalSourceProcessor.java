package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.async.decorator.async.HttpDecorator;
import com.gojek.de.stencil.StencilClient;
import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.Constants.*;

public class ExternalSourceProcessor implements PostProcessor {


    private Configuration configuration;
    private StencilClient stencilClient;
    private Map<String, Object> externalSourceConfig;

    public ExternalSourceProcessor(Configuration configuration, StencilClient stencilClient) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        String externalSourceConfigString = configuration.getString(EXTERNAL_SOURCE_KEY, "");
        externalSourceConfig = new Gson().fromJson(externalSourceConfigString, Map.class);
        DataStream<Row> resultStream = streamInfo.getDataStream();

        String[] inputColumnNames = streamInfo.getColumnNames();

        String[] outputColumnNames = getColumnNames(inputColumnNames);

        for (String type : externalSourceConfig.keySet()) {
            ArrayList<Map<String, Object>> requests = (ArrayList) externalSourceConfig.get(type);
            String outputProto = outputProto();

            for (Map<String, Object> requestMap : requests) {
                Integer asyncIOCapacity = Integer.valueOf(configuration.getString(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT));
                HttpDecorator httpDecorator = new HttpDecorator(requestMap, stencilClient, asyncIOCapacity, type, inputColumnNames, outputProto);
                resultStream = httpDecorator.decorate(resultStream);
            }
        }
        return new StreamInfo(resultStream, outputColumnNames);
    }

    private String[] getColumnNames(String[] inputColumnNames) {
        List<String> outputColumnNames = new ArrayList<>(Arrays.asList(inputColumnNames));
        for (String type : externalSourceConfig.keySet()) {
            ArrayList<Map<String, Object>> requests = (ArrayList) externalSourceConfig.get(type);
            for (Map<String, Object> requestMap : requests) {
                Map<String, Object> outputMaps = (Map<String, Object>) requestMap.get(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY);
                outputColumnNames.addAll(outputMaps.keySet());
            }
        }
        return outputColumnNames.toArray(new String[0]);
    }

    // TODO: [PORTAL_MIGRATION] Remove this switch when migration to new portal is done
    private String outputProto() {
        // [PORTAL_MIGRATION] Move content inside this block to process method
        if (configuration.getString(PORTAL_VERSION, "1").equals("2")) {
            return configuration.getString(OUTPUT_PROTO_MESSAGE, "");
        }

        String outputProtoPrefix = configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "");
        return String.format("%sMessage", outputProtoPrefix);
    }

}
