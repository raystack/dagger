package com.gojek.daggers.postProcessors.external;

import com.gojek.daggers.postProcessors.external.common.SourceConfig;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;

import java.util.ArrayList;
import java.util.List;

public class ExternalSourceConfig {
    List<HttpSourceConfig> http;
    List<EsSourceConfig> es;

    public ExternalSourceConfig(List<HttpSourceConfig> http, List<EsSourceConfig> es) {
        this.http = http;
        this.es = es;
    }

    public List<HttpSourceConfig> getHttpConfig() {
        return http == null ? new ArrayList<>() : http;
    }

    public List<EsSourceConfig> getEsConfig() {
        return es == null ? new ArrayList<>() : es;
    }

    public boolean isEmpty() {
        return (http == null || http.isEmpty()) && (es == null || es.isEmpty());
    }

    public List<String> getOutputColumnNames() {
        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.addAll(getOutputColumnNames(http));
        columnNames.addAll(getOutputColumnNames(es));
        return columnNames;
    }

    private <T extends SourceConfig> ArrayList<String> getOutputColumnNames(List<T> configs) {
        ArrayList<String> columnNames = new ArrayList<>();
        if (configs == null)
            return columnNames;
        configs.forEach(config -> columnNames.addAll(config.getOutputColumns()));
        return columnNames;
    }
}
