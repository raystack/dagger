package io.odpf.dagger.core.processors.external;

import io.odpf.dagger.core.processors.types.SourceConfig;
import io.odpf.dagger.core.processors.external.es.EsSourceConfig;
import io.odpf.dagger.core.processors.external.grpc.GrpcSourceConfig;
import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import io.odpf.dagger.core.processors.external.pg.PgSourceConfig;

import java.util.ArrayList;
import java.util.List;

public class ExternalSourceConfig {
    private List<HttpSourceConfig> http;
    private List<EsSourceConfig> es;
    private List<PgSourceConfig> pg;
    private List<GrpcSourceConfig> grpc;

    public ExternalSourceConfig(List<HttpSourceConfig> http, List<EsSourceConfig> es, List<PgSourceConfig> pg, List<GrpcSourceConfig> grpc) {
        this.http = http;
        this.es = es;
        this.pg = pg;
        this.grpc = grpc;
    }

    public List<HttpSourceConfig> getHttpConfig() {
        return http == null ? new ArrayList<>() : http;
    }

    public List<EsSourceConfig> getEsConfig() {
        return es == null ? new ArrayList<>() : es;
    }

    public List<PgSourceConfig> getPgConfig() {
        return pg == null ? new ArrayList<>() : pg;
    }

    public List<GrpcSourceConfig> getGrpcConfig() {
        return grpc == null ? new ArrayList<>() : grpc;
    }


    public boolean isEmpty() {
        return (http == null || http.isEmpty()) && (es == null || es.isEmpty()) && (pg == null || pg.isEmpty()) && (grpc == null || grpc.isEmpty());
    }

    public List<String> getOutputColumnNames() {
        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.addAll(getOutputColumnNames(http));
        columnNames.addAll(getOutputColumnNames(es));
        columnNames.addAll(getOutputColumnNames(pg));
        columnNames.addAll(getOutputColumnNames(grpc));
        return columnNames;
    }

    private <T extends SourceConfig> ArrayList<String> getOutputColumnNames(List<T> configs) {
        ArrayList<String> columnNames = new ArrayList<>();
        if (configs == null) {
            return columnNames;
        }
        configs.forEach(config -> columnNames.addAll(config.getOutputColumns()));
        return columnNames;
    }

}
