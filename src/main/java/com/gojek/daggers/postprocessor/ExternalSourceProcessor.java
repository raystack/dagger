package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.async.decorator.async.HttpDecorator;
import com.gojek.de.stencil.StencilClient;
import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Map;

import static com.gojek.daggers.Constants.*;

public class ExternalSourceProcessor implements PostProcessor {


    private Configuration configuration;
    private StencilClient stencilClient;

    public ExternalSourceProcessor(Configuration configuration, StencilClient stencilClient) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        String externalSourceConfigString = configuration.getString(EXTERNAL_SOURCE_KEY, "");
        Map<String, Object> externalSourceConfig = new Gson().fromJson(externalSourceConfigString, Map.class);
        DataStream<Row> resultStream = streamInfo.getDataStream();

        String[] columnNames = streamInfo.getColumnNames();
        for (String type : externalSourceConfig.keySet()) {
            ArrayList<Map<String, Object>> requests = (ArrayList) externalSourceConfig.get(type);
            String outputProto = outputProto();

            for (Map<String, Object> requestMap : requests) {
                Integer asyncIOCapacity = Integer.valueOf(configuration.getString(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT));
                HttpDecorator httpDecorator = new HttpDecorator(requestMap, stencilClient, asyncIOCapacity, type, columnNames, outputProto);
                resultStream = httpDecorator.decorate(resultStream);
            }
        }
        return new StreamInfo(resultStream, columnNames);
    }

    // TODO: [PORTAL_MIGRATION] Remove this switch when migration to new portal is done
    private String outputProto() {
        // [PORTAL_MIGRATION] Move conteont inside this block to process method
        if (configuration.getString(PORTAL_VERSION, "1").equals("2")) {
            return configuration.getString(OUTPUT_PROTO_MESSAGE, "");
        }

        String outputProtoPrefix = configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "");
        return String.format("%sMessage", outputProtoPrefix);
    }

}
