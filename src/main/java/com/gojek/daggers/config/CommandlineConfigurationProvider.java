package com.gojek.daggers.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

public class CommandlineConfigurationProvider implements ConfigurationProvider {

    private String[] args;

    public CommandlineConfigurationProvider(String[] args) {

        this.args = args;
    }

    @Override
    public Configuration get() {
        System.out.println("params from " + CommandlineConfigurationProvider.class.getName());
        ParameterTool.fromArgs(args).toMap().entrySet().stream().forEach(e -> System.out.println(e));
        return ParameterTool.fromArgs(args).getConfiguration();
    }
}
