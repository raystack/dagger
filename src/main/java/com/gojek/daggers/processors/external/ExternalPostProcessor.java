package com.gojek.daggers.processors.external;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.processors.types.PostProcessor;
import com.gojek.daggers.processors.PostProcessorConfig;
import com.gojek.daggers.processors.types.Validator;
import com.gojek.daggers.processors.types.SourceConfig;
import com.gojek.daggers.processors.types.StreamDecorator;
import com.gojek.daggers.processors.external.es.EsSourceConfig;
import com.gojek.daggers.processors.external.es.EsStreamDecorator;
import com.gojek.daggers.processors.external.grpc.GrpcSourceConfig;
import com.gojek.daggers.processors.external.grpc.GrpcStreamDecorator;
import com.gojek.daggers.processors.external.http.HttpSourceConfig;
import com.gojek.daggers.processors.external.http.HttpStreamDecorator;
import com.gojek.daggers.processors.external.pg.PgSourceConfig;
import com.gojek.daggers.processors.external.pg.PgStreamDecorator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;

public class ExternalPostProcessor implements PostProcessor {

    private final SchemaConfig schemaConfig;
    private final ExternalSourceConfig externalSourceConfig;
    private final ExternalMetricConfig externalMetricConfig;

    public ExternalPostProcessor(SchemaConfig schemaConfig, ExternalSourceConfig externalSourceConfig, ExternalMetricConfig externalMetricConfig) {
        this.schemaConfig = schemaConfig;
        this.externalSourceConfig = externalSourceConfig;
        this.externalMetricConfig = externalMetricConfig;
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
            resultStream = enrichStream(resultStream, httpSourceConfig, getHttpDecorator(httpSourceConfig));
        }

        List<EsSourceConfig> esSourceConfigs = externalSourceConfig.getEsConfig();
        for (int index = 0; index < esSourceConfigs.size(); index++) {
            EsSourceConfig esSourceConfig = esSourceConfigs.get(index);
            externalMetricConfig.setMetricId(getMetricId(index, esSourceConfig));
            resultStream = enrichStream(resultStream, esSourceConfig, getEsDecorator(esSourceConfig));
        }

        List<PgSourceConfig> pgSourceConfigs = externalSourceConfig.getPgConfig();
        for (int index = 0; index < pgSourceConfigs.size(); index++) {
            PgSourceConfig pgSourceConfig = pgSourceConfigs.get(index);
            externalMetricConfig.setMetricId(getMetricId(index, pgSourceConfig));
            resultStream = enrichStream(resultStream, pgSourceConfig, getPgDecorator(pgSourceConfig));
        }

        List<GrpcSourceConfig> grpcSourceConfigs = externalSourceConfig.getGrpcConfig();
        for (int index = 0; index < grpcSourceConfigs.size(); index++) {
            GrpcSourceConfig grpcSourceConfig = grpcSourceConfigs.get(index);
            externalMetricConfig.setMetricId(getMetricId(index, grpcSourceConfig));
            resultStream = enrichStream(resultStream, grpcSourceConfig, getGrpcDecorator(grpcSourceConfig));
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

    protected HttpStreamDecorator getHttpDecorator(HttpSourceConfig httpSourceConfig) {
        return new HttpStreamDecorator(httpSourceConfig, externalMetricConfig, schemaConfig);
    }

    protected EsStreamDecorator getEsDecorator(EsSourceConfig esSourceConfig) {
        return new EsStreamDecorator(esSourceConfig, externalMetricConfig, schemaConfig);
    }

    private PgStreamDecorator getPgDecorator(PgSourceConfig pgSourceConfig) {
        return new PgStreamDecorator(pgSourceConfig, externalMetricConfig, schemaConfig);
    }

    private GrpcStreamDecorator getGrpcDecorator(GrpcSourceConfig grpcSourceConfig) {
        return new GrpcStreamDecorator(grpcSourceConfig, externalMetricConfig, schemaConfig);
    }


}
