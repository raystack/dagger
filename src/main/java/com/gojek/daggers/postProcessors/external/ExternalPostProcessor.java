package com.gojek.daggers.postProcessors.external;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.common.Validator;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.es.EsStreamDecorator;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpStreamDecorator;
import com.gojek.daggers.postProcessors.external.pg.PgSourceConfig;
import com.gojek.daggers.postProcessors.external.pg.PgStreamDecorator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;

public class ExternalPostProcessor implements PostProcessor {

    private StencilClientOrchestrator stencilClientOrchestrator;
    private ExternalSourceConfig externalSourceConfig;
    private ColumnNameManager columnNameManager;
    private TelemetrySubscriber telemetrySubscriber;
    private boolean telemetryEnabled;
    private long shutDownPeriod;

    public ExternalPostProcessor(StencilClientOrchestrator stencilClientOrchestrator, ExternalSourceConfig externalSourceConfig,
                                 ColumnNameManager columnNameManager, TelemetrySubscriber telemetrySubscriber, boolean telemetryEnabled, long shutDownPeriod) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.externalSourceConfig = externalSourceConfig;
        this.columnNameManager = columnNameManager;
        this.telemetrySubscriber = telemetrySubscriber;
        this.telemetryEnabled = telemetryEnabled;
        this.shutDownPeriod = shutDownPeriod;
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
            resultStream = enrichStream(resultStream, httpSourceConfig, getHttpDecorator(httpSourceConfig, columnNameManager, telemetrySubscriber));
        }

        List<EsSourceConfig> esSourceConfigs = externalSourceConfig.getEsConfig();
        for (EsSourceConfig esSourceConfig : esSourceConfigs) {
            resultStream = enrichStream(resultStream, esSourceConfig, getEsDecorator(esSourceConfig, columnNameManager, telemetrySubscriber));
        }

        List<PgSourceConfig> pgSourceConfigs = externalSourceConfig.getPgConfig();
        for (PgSourceConfig pgSourceConfig : pgSourceConfigs) {
            resultStream = enrichStream(resultStream, pgSourceConfig, getPgDecorator(pgSourceConfig, columnNameManager, telemetrySubscriber));
        }

        return new StreamInfo(resultStream, streamInfo.getColumnNames());
    }


    private DataStream<Row> enrichStream(DataStream<Row> resultStream, Validator configs, StreamDecorator decorator) {
        configs.validateFields();
        return decorator.decorate(resultStream);
    }

    protected HttpStreamDecorator getHttpDecorator(HttpSourceConfig httpSourceConfig, ColumnNameManager columnNameManager, TelemetrySubscriber telemetrySubscriber) {
        return new HttpStreamDecorator(httpSourceConfig, stencilClientOrchestrator, columnNameManager, telemetrySubscriber, telemetryEnabled, shutDownPeriod);

    }

    protected EsStreamDecorator getEsDecorator(EsSourceConfig esSourceConfig, ColumnNameManager columnNameManager, TelemetrySubscriber telemetrySubscriber) {
        return new EsStreamDecorator(esSourceConfig, stencilClientOrchestrator, columnNameManager, telemetrySubscriber, telemetryEnabled, shutDownPeriod);
    }

    private PgStreamDecorator getPgDecorator(PgSourceConfig pgSourceConfig, ColumnNameManager columnNameManager, TelemetrySubscriber telemetrySubscriber) {
        return new PgStreamDecorator(pgSourceConfig, stencilClientOrchestrator, columnNameManager, telemetrySubscriber, telemetryEnabled, shutDownPeriod);
    }
}
