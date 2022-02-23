package io.odpf.dagger.core.processors.external;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.PostProcessorConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.processors.types.SourceConfig;
import io.odpf.dagger.core.processors.types.StreamDecorator;
import io.odpf.dagger.core.processors.types.Validator;
import io.odpf.dagger.core.processors.external.es.EsSourceConfig;
import io.odpf.dagger.core.processors.external.es.EsStreamDecorator;
import io.odpf.dagger.core.processors.external.grpc.GrpcSourceConfig;
import io.odpf.dagger.core.processors.external.grpc.GrpcStreamDecorator;
import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import io.odpf.dagger.core.processors.external.http.HttpStreamDecorator;
import io.odpf.dagger.core.processors.external.pg.PgSourceConfig;
import io.odpf.dagger.core.processors.external.pg.PgStreamDecorator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * The External post processor.
 */
public class ExternalPostProcessor implements PostProcessor {

    private final SchemaConfig schemaConfig;
    private final ExternalSourceConfig externalSourceConfig;
    private final ExternalMetricConfig externalMetricConfig;

    /**
     * Instantiates a new External post processor.
     *
     * @param schemaConfig         the schema config
     * @param externalSourceConfig the external source config
     * @param externalMetricConfig the external metric config
     */
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

    /**
     * Gets http decorator.
     *
     * @param httpSourceConfig the http source config
     * @return the http decorator
     */
    protected HttpStreamDecorator getHttpDecorator(HttpSourceConfig httpSourceConfig) {
        return new HttpStreamDecorator(httpSourceConfig, externalMetricConfig, schemaConfig);
    }

    /**
     * Gets es decorator.
     *
     * @param esSourceConfig the es source config
     * @return the es decorator
     */
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
