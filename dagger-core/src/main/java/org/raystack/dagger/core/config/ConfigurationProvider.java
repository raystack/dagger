package org.raystack.dagger.core.config;

import org.raystack.dagger.common.configuration.Configuration;

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
