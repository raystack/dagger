package com.gojek.daggers.config;

import com.gojek.daggers.DaggerConfigurationException;
import org.slf4j.Logger;

import java.util.Arrays;

public class ConfigurationProviderFactory {

    private String[] args;

    public ConfigurationProviderFactory(String[] args) {

        this.args = args;
        System.out.println("Arguments are : ");
        Arrays.asList(args).stream().forEach(s -> {
            System.out.println(s);
        });
    }

    public ConfigurationProvider Provider() {
        if (System.getProperties().containsKey("ConfigSource")) {
            String configSource = System.getProperty("ConfigSource");
            switch (configSource) {
                case "ENVIRONMENT":
                    return new EnvironmentConfigurationProvider(System.getenv());
                case "ARGS":
                    return new CommandlineConfigurationProvider(args);
                default:
                    throw new DaggerConfigurationException("Config source not provided");

            }
        }
        return new CommandlineConfigurationProvider(args);
    }
}
