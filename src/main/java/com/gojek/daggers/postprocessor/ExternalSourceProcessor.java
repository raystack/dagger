package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.async.decorator.async.HttpDecorator;
import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.daggers.postprocessor.parser.PostProcessorConfig;
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
    private PostProcessorConfig postProcessorConfig;
    public ExternalSourceProcessor(Configuration configuration, StencilClient stencilClient, PostProcessorConfig postProcessorConfig) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
        this.postProcessorConfig = postProcessorConfig;
    }

    @Override
    public boolean canProcess(Configuration configuration, PostProcessorConfig postProcessorConfig) {
        return postProcessorConfig.getExternalSource() != null;
    }


    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        DataStream<Row> resultStream = streamInfo.getDataStream();
        String[] outputColumnNames = getColumnNames(postProcessorConfig, streamInfo.getColumnNames());
        List<HttpExternalSourceConfig> httpExternalSourceConfigs = postProcessorConfig.getExternalSource().getHttpConfig();
        for (HttpExternalSourceConfig httpExternalSourceConfig : httpExternalSourceConfigs) {
            httpExternalSourceConfig.validateFields();
            Integer asyncIOCapacity = Integer.valueOf(configuration.getString(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT));
            HttpDecorator httpDecorator = getHttpDecorator(outputColumnNames, "http", httpExternalSourceConfig, asyncIOCapacity);
            resultStream = httpDecorator.decorate(resultStream);
        }
        return new StreamInfo(resultStream, outputColumnNames);
    }

    protected HttpDecorator getHttpDecorator(String[] outputColumnNames, String type, HttpExternalSourceConfig httpExternalSourceConfig, Integer asyncIOCapacity) {
        return new HttpDecorator(httpExternalSourceConfig, stencilClient, asyncIOCapacity, type, outputColumnNames);
    }

    private String[] getColumnNames(PostProcessorConfig postProcessorConfig, String[] inputColumnNames) {
        List<String> outputColumnNames = new ArrayList<>(Arrays.asList(inputColumnNames));
        outputColumnNames.addAll(postProcessorConfig.getColumns());
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
