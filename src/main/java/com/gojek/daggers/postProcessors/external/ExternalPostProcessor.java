package com.gojek.daggers.postProcessors.external;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.common.Validator;
import com.gojek.daggers.postProcessors.external.common.SourceConfig;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.es.EsStreamDecorator;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpStreamDecorator;
import com.gojek.daggers.postProcessors.external.pg.PgSourceConfig;
import com.gojek.daggers.postProcessors.external.pg.PgStreamDecorator;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class ExternalPostProcessor implements PostProcessor {

    private StencilClientOrchestrator stencilClientOrchestrator;
    private ExternalSourceConfig externalSourceConfig;
    private ColumnNameManager columnNameManager;
    private ExternalMetricConfig externalMetricConfig;
    private String[] inputProtoClasses;

    public ExternalPostProcessor(StencilClientOrchestrator stencilClientOrchestrator, ExternalSourceConfig externalSourceConfig,
                                 ColumnNameManager columnNameManager, ExternalMetricConfig externalMetricConfig, String[] inputProtoClasses) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.externalSourceConfig = externalSourceConfig;
        this.columnNameManager = columnNameManager;
        this.externalMetricConfig = externalMetricConfig;
        this.inputProtoClasses = inputProtoClasses;
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return postProcessorConfig.hasExternalSource();
    }


    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        DataStream<Row> resultStream = streamInfo.getDataStream();

        List<HttpSourceConfig> httpSourceConfigs = externalSourceConfig.getHttpConfig();
        for (int index = 0; index < httpSourceConfigs.size(); index++) {
            HttpSourceConfig httpSourceConfig = httpSourceConfigs.get(index);
            externalMetricConfig.setMetricId(getMetricId(index, httpSourceConfig));
            resultStream = enrichStream(resultStream, httpSourceConfig, getHttpDecorator(httpSourceConfig, columnNameManager, externalMetricConfig));
        }

        List<EsSourceConfig> esSourceConfigs = externalSourceConfig.getEsConfig();
        for (int index = 0; index < esSourceConfigs.size(); index++) {
            EsSourceConfig esSourceConfig = esSourceConfigs.get(index);
            externalMetricConfig.setMetricId(getMetricId(index, esSourceConfig));
            resultStream = enrichStream(resultStream, esSourceConfig, getEsDecorator(esSourceConfig, columnNameManager, externalMetricConfig));
        }

        List<PgSourceConfig> pgSourceConfigs = externalSourceConfig.getPgConfig();
        for (int index = 0; index < pgSourceConfigs.size(); index++) {
            PgSourceConfig pgSourceConfig = pgSourceConfigs.get(index);
            externalMetricConfig.setMetricId(getMetricId(index, pgSourceConfig));
            resultStream = enrichStream(resultStream, pgSourceConfig, getPgDecorator(pgSourceConfig, columnNameManager, externalMetricConfig));
        }

        return new StreamInfo(resultStream, streamInfo.getColumnNames());
    }

    private String getMetricId(int index, SourceConfig sourceConfig) {
        String metricId = sourceConfig.getMetricId();
        return (StringUtils.isEmpty(metricId)) ? String.valueOf(index) : metricId;
    }

    private DataStream<Row> enrichStream(DataStream<Row> resultStream, Validator configs, StreamDecorator decorator) {
        configs.validateFields();
        return decorator.decorate(resultStream);
    }

    protected HttpStreamDecorator getHttpDecorator(HttpSourceConfig httpSourceConfig, ColumnNameManager columnNameManager, ExternalMetricConfig externalMetricConfig) {
        return new HttpStreamDecorator(httpSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
    }

    protected EsStreamDecorator getEsDecorator(EsSourceConfig esSourceConfig, ColumnNameManager columnNameManager, ExternalMetricConfig externalMetricConfig) {
        return new EsStreamDecorator(esSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
    }

    private PgStreamDecorator getPgDecorator(PgSourceConfig pgSourceConfig, ColumnNameManager columnNameManager, ExternalMetricConfig externalMetricConfig) {
        return new PgStreamDecorator(pgSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
    }
}
