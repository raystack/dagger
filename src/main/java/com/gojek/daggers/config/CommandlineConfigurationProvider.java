package com.gojek.daggers.config;

import com.google.gson.Gson;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

public class CommandlineConfigurationProvider implements ConfigurationProvider {

    private String[] args;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandlineConfigurationProvider.class.getName());

    public CommandlineConfigurationProvider(String[] args) {
        this.args = args;
    }

    @Override
    public Configuration get() {
        LOGGER.info("params from " + CommandlineConfigurationProvider.class.getName());
        if (args.length == 0) {
            return ParameterTool.fromArgs(args).getConfiguration();
        }
        byte[] decodedArgs = Base64.getMimeDecoder().decode(args[0]);
        String[] arrOfArgs = new Gson().fromJson(new String(decodedArgs), String[].class);
        ParameterTool.fromArgs(arrOfArgs).toMap().entrySet().stream().forEach(e -> LOGGER.info(e.toString()));
        return ParameterTool.fromArgs(arrOfArgs).getConfiguration();
    }
}
