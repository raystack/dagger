package com.gojek.daggers.config;

import com.gojek.daggers.exception.DaggerConfigurationException;

import java.util.Arrays;

public class ConfigurationProviderFactory {

    private String[] args;

    public ConfigurationProviderFactory(String[] args) {

        this.args = args;
        System.out.println("Arguments are : ");
        Arrays.asList(args).stream().forEach(s -> {
            System.out.println(s);
            if(s.contains("ConfigFile")) {
                System.setProperty("ConfigSource", "FILE");
                System.setProperty("DAGGER_CONFIG_PATH", s.split("=")[1]);
            }
        });

    }

    public ConfigurationProvider provider() {
        if (System.getProperties().containsKey("ConfigSource")) {
            String configSource = System.getProperty("ConfigSource");
            switch (configSource) {
                case "ENVIRONMENT":
                    return new EnvironmentConfigurationProvider(System.getenv());
                case "ARGS":
                    return new CommandlineConfigurationProvider(args);
                case "FILE":
                    return new FileConfigurationProvider();
                default:
                    throw new DaggerConfigurationException("Config source not provided");

            }
        }
        return new CommandlineConfigurationProvider(args);
    }
}
