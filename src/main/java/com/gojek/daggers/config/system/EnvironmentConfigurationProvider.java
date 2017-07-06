package com.gojek.daggers.config.system;

import com.gojek.daggers.config.ConfigurationProvider;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Map;

public class EnvironmentConfigurationProvider implements ConfigurationProvider {

    @Override
    public Map<String, String> get(String input) {
        return ParameterTool.fromSystemProperties().toMap();
    }
}
