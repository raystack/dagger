package io.odpf.dagger.core.config;

import org.apache.flink.configuration.Configuration;

/**
 * The interface for all Configuration provider class.
 */
public interface ConfigurationProvider {
    /**
     * Get configuration.
     *
     * @return the configuration
     */
    Configuration get();
}
