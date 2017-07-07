package com.gojek.daggers.config;

import com.gojek.daggers.DaggerConfigurationException;
import com.gojek.daggers.config.commandline.CommandlineConfigurationProvider;
import com.gojek.daggers.config.system.EnvironmentConfigurationProvider;

import java.util.Map;

public class ConfigurationProviderFactory{

    private String[] args;

    public ConfigurationProviderFactory(String[] args) {

        this.args = args;
    }

    public ConfigurationProvider Provider() {
        switch (System.getProperty("ConfigSource")){
            case "ENVIRONMENT":
                return new EnvironmentConfigurationProvider();
            case "ARGS":
                return new CommandlineConfigurationProvider(args);
            default:
                throw new DaggerConfigurationException("Config source not provided");

        }
    }
}
