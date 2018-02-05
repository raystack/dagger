package com.gojek.daggers.config;

import org.apache.flink.configuration.Configuration;

public interface ConfigurationProvider {
    Configuration get();
}
