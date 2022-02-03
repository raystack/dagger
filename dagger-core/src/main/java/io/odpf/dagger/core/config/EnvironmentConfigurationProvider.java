package io.odpf.dagger.core.config;

import org.apache.flink.api.java.utils.ParameterTool;

import io.odpf.dagger.common.configuration.Configuration;

import java.util.Map;

/**
 * The class which handle configuration provided from Environment.
 */
public class EnvironmentConfigurationProvider implements ConfigurationProvider {

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
    public Configuration get() {
        return new Configuration(ParameterTool.fromMap(environmentParameters));
    }
}
