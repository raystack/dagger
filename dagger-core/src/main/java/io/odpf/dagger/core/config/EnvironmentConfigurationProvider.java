package io.odpf.dagger.core.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

/**
 * The class which handle configuration provided from Environment.
 */
public class EnvironmentConfigurationProvider implements UserConfigurationProvider {

    private Map<String, String> environmentParameters;

    /**
     * Instantiates a new Environment configuration provider.
     *
     * @param environmentParameters the environment parameters
     */
    public EnvironmentConfigurationProvider(Map<String, String> environmentParameters) {

        this.environmentParameters = environmentParameters;
    }

    @Override
    public ParameterTool get() {
        return ParameterTool.fromMap(environmentParameters);
    }
}
