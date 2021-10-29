package io.odpf.dagger.core.config;

import io.odpf.dagger.common.configuration.Configuration;

/**
 * The interface for all Configuration provider class.
 */
public interface ConfigurationProvider {
    /**
     * Get conf.
     *
     * @return the configuration
     */
    Configuration getConfiguration();
}
