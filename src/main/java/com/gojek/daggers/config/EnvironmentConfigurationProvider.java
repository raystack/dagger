package com.gojek.daggers.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

public class EnvironmentConfigurationProvider implements ConfigurationProvider {

    private Map<String, String> environmentParameters;

    public EnvironmentConfigurationProvider(Map<String, String> environmentParameters) {

        this.environmentParameters = environmentParameters;
    }

    @Override
    public Configuration get() {
        return ParameterTool.fromMap(environmentParameters).getConfiguration();
    }
}
