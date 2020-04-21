package com.gojek.daggers.postProcessors.external.pg;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.SourceConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PgSourceConfig implements Serializable, SourceConfig {

    private final String host;
    private final String port;
    private final String user;
    private final String password;
    private final String database;
    private final String type;
    private final String capacity;
    private final String streamTimeout;
    private final Map<String, OutputMapping> outputMapping;

    public PgSourceConfig(String host, String port, String user, String password, String database,
                          String type, String capacity, String streamTimeout, Map<String, OutputMapping> outputMapping) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = database;
        this.type = type;
        this.capacity = capacity;
        this.streamTimeout = streamTimeout;
        this.outputMapping = outputMapping;
    }

    @Override
    public List<String> getOutputColumns() {
        return new ArrayList<>(outputMapping.keySet());
    }

    @Override
    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("host", host);
        mandatoryFields.put("port", port);
        mandatoryFields.put("user", user);
        mandatoryFields.put("password", password);
        mandatoryFields.put("database", database);
        mandatoryFields.put("type", type);
        mandatoryFields.put("capacity", capacity);
        mandatoryFields.put("stream_timeout", streamTimeout);
        mandatoryFields.put("outputMapping", outputMapping);

        return mandatoryFields;
    }
}
