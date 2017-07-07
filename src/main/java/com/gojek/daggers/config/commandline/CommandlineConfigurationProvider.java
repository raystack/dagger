package com.gojek.daggers.config.commandline;

import com.gojek.daggers.config.ConfigurationProvider;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

public class CommandlineConfigurationProvider implements ConfigurationProvider {

    private String[] args;

    public CommandlineConfigurationProvider(String[] args) {

        this.args = args;
    }

    @Override
    public Map<String, String> get() {
        return ParameterTool.fromArgs(args).toMap();
    }
}
