package io.odpf.dagger.common.configuration;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public class UserConfiguration implements Serializable {
    private final ParameterTool param;

    public UserConfiguration(ParameterTool param) {
        this.param = param;
    }

    public ParameterTool getParam() {
        return param;
    }
}
