package com.gotocompany.dagger.core.config;

import com.gotocompany.dagger.common.configuration.Configuration;

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
