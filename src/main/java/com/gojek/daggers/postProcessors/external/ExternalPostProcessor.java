package com.gojek.daggers.postProcessors.external;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.common.Validator;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.es.EsStreamDecorator;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpStreamDecorator;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;

public class ExternalPostProcessor implements PostProcessor {

    private StencilClient stencilClient;
    private ExternalSourceConfig externalSourceConfig;
    private ColumnNameManager columnNameManager;

    public ExternalPostProcessor(StencilClient stencilClient, ExternalSourceConfig externalSourceConfig, ColumnNameManager columnNameManager) {
        this.stencilClient = stencilClient;
        this.externalSourceConfig = externalSourceConfig;
        this.columnNameManager = columnNameManager;
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return postProcessorConfig.hasExternalSource();
    }


    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        DataStream<Row> resultStream = streamInfo.getDataStream();

        List<HttpSourceConfig> httpSourceConfigs = externalSourceConfig.getHttpConfig();
        for (HttpSourceConfig httpSourceConfig : httpSourceConfigs) {
            resultStream = enrichStream(resultStream, httpSourceConfig, getHttpDecorator(httpSourceConfig, columnNameManager));
        }

        List<EsSourceConfig> esSourceConfigs = externalSourceConfig.getEsConfig();
        for (EsSourceConfig esSourceConfig : esSourceConfigs) {
            resultStream = enrichStream(resultStream, esSourceConfig, getEsDecorator(esSourceConfig, columnNameManager));
        }

        return new StreamInfo(resultStream, streamInfo.getColumnNames());
    }

    private DataStream<Row> enrichStream(DataStream<Row> resultStream, Validator configs, StreamDecorator decorator) {
        configs.validateFields();
        return decorator.decorate(resultStream);
    }


    protected HttpStreamDecorator getHttpDecorator(HttpSourceConfig httpSourceConfig, ColumnNameManager columnNameManager) {
        return new HttpStreamDecorator(httpSourceConfig, stencilClient, columnNameManager);
    }

    protected EsStreamDecorator getEsDecorator(EsSourceConfig esSourceConfig, ColumnNameManager columnNameManager) {
        return new EsStreamDecorator(esSourceConfig, stencilClient, columnNameManager);
    }
}
