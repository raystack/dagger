package io.odpf.dagger.core.config;

import org.apache.flink.configuration.Configuration;

public interface ConfigurationProvider {
    Configuration get();
}
