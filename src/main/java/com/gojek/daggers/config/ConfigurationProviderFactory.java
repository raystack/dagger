package com.gojek.daggers.config;

import com.gojek.daggers.DaggerConfigurationException;

public class ConfigurationProviderFactory{

    private String[] args;

    public ConfigurationProviderFactory(String[] args) {

        this.args = args;
    }

    public ConfigurationProvider Provider() {
        switch (System.getProperty("ConfigSource")){
            case "ENVIRONMENT":
                return new EnvironmentConfigurationProvider(System.getenv());
            case "ARGS":
                return new CommandlineConfigurationProvider(args);
            default:
                throw new DaggerConfigurationException("Config source not provided");

        }
    }
}
