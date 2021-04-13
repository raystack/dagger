package io.odpf.dagger.config;

import org.apache.flink.configuration.Configuration;

public interface ConfigurationProvider {
    Configuration get();
}
