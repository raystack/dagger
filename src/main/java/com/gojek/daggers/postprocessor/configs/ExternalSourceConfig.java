package com.gojek.daggers.postprocessor.configs;

import java.util.ArrayList;
import java.util.List;

public class ExternalSourceConfig {
    List<HttpExternalSourceConfig> http;

    public ExternalSourceConfig(List<HttpExternalSourceConfig> http) {
        this.http = http;
    }

    public List<HttpExternalSourceConfig> getHttpConfig() {
        return http;
    }

    public boolean isEmpty() {
        return http == null;
    }

    public List<String> getColumnNames(){
        ArrayList<String> columnNames = new ArrayList<>();
        http.forEach(httpExternalSourceConfig -> columnNames.addAll(httpExternalSourceConfig.getColumns()));
        return columnNames;
    }
}
