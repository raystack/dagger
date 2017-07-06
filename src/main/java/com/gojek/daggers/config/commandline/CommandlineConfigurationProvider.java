package com.gojek.daggers.config.commandline;

import com.gojek.daggers.config.ConfigurationProvider;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Map;

public class CommandlineConfigurationProvider implements ConfigurationProvider {

    @Override
    public Map<String, String> get(String input) {
        if (input != null && !input.isEmpty()) {
            return ParameterTool.fromArgs(input.trim().split(" ")).toMap();
        } else {
            return Maps.newHashMap();
        }
    }
}
