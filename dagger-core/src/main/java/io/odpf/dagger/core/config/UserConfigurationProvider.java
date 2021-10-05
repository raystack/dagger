package io.odpf.dagger.core.config;

import io.odpf.dagger.common.configuration.UserConfiguration;

/**
 * The interface for all Configuration provider class.
 */
public interface UserConfigurationProvider {
    /**
     * Get userConf
     *
     * @return the userConfiguration
     */
    UserConfiguration getUserConf();
}
