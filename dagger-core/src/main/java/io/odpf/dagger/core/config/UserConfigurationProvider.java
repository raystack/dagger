package io.odpf.dagger.core.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 * The interface for all Configuration provider class.
 */
public interface UserConfigurationProvider {
    /**
     * Get configuration.
     *
     * @return the configuration
     */
    ParameterTool get();
}
