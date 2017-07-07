package com.gojek.daggers.config.system;

import com.gojek.daggers.config.ConfigurationProvider;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

public class EnvironmentConfigurationProvider implements ConfigurationProvider {

    @Override
    public Configuration get() {
        return ParameterTool.fromSystemProperties().getConfiguration();
    }
}
