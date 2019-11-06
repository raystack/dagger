package com.gojek.daggers.postprocessor.parser;

import java.util.ArrayList;
import java.util.List;

public class ExternalSourceConfig {
    List<HttpExternalSourceConfig> http;
    List<EsExternalSourceConfig> es;

    ExternalSourceConfig(List<HttpExternalSourceConfig> http, List<EsExternalSourceConfig> es) {
        this.http = http;
        this.es = es;
    }

    public List<HttpExternalSourceConfig> getHttpConfig() {
        return http;
    }
    public List<EsExternalSourceConfig> getEsConfig() { return es; }

    public boolean isEmpty() {
        return http == null && es == null;
    }

    public List<String> getColumnNames(){
        ArrayList<String> columnNames = new ArrayList<>();
        http.forEach(httpExternalSourceConfig -> columnNames.addAll(httpExternalSourceConfig.getColumns()));
        return columnNames;
    }
}
