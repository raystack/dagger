package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.async.decorator.async.HttpDecorator;
import com.gojek.daggers.postprocessor.parser.ExternalSourceConfig;
import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.gojek.daggers.Constants.*;

public class ExternalSourceProcessor implements PostProcessor {


    private Configuration configuration;
    private StencilClient stencilClient;
    private HttpDecorator httpDecorator;

    public ExternalSourceProcessor(Configuration configuration, StencilClient stencilClient) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
    }

    public ExternalSourceProcessor(Configuration configuration, StencilClient stencilClient, HttpDecorator httpDecorator) {
        this(configuration, stencilClient);
        this.httpDecorator = httpDecorator;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        String externalSourceConfigString = configuration.getString(EXTERNAL_SOURCE_KEY, "");
        ExternalSourceConfig externalSourceConfig = ExternalSourceConfig.parse(externalSourceConfigString);
        DataStream<Row> resultStream = streamInfo.getDataStream();
        String[] outputColumnNames = getColumnNames(externalSourceConfig, streamInfo.getColumnNames());

        for (String type : externalSourceConfig.getExternalSourceKeys()) {
            List<HttpExternalSourceConfig> httpExternalSourceConfigs = externalSourceConfig.getHttpExternalSourceConfig();
            String outputProto = outputProto();

            for (HttpExternalSourceConfig httpExternalSourceConfig : httpExternalSourceConfigs) {
                httpExternalSourceConfig.validate();
                Integer asyncIOCapacity = Integer.valueOf(configuration.getString(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT));
                if (httpDecorator == null)
                    httpDecorator = new HttpDecorator(httpExternalSourceConfig, stencilClient, asyncIOCapacity, type, outputColumnNames, outputProto);
                resultStream = httpDecorator.decorate(resultStream);
            }
        }
        return new StreamInfo(resultStream, outputColumnNames);
    }

    private String[] getColumnNames(ExternalSourceConfig externalSourceConfig, String[] inputColumnNames) {
        List<String> outputColumnNames = new ArrayList<>(Arrays.asList(inputColumnNames));
        outputColumnNames.addAll(externalSourceConfig.getColumns());
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
