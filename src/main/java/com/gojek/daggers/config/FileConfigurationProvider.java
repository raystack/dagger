package com.gojek.daggers.config;

import com.gojek.daggers.exception.DaggerConfigurationException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class  FileConfigurationProvider  implements ConfigurationProvider {

    public FileConfigurationProvider() {
        this.environmentParameters = new HashMap<>();

        String daggerPropertiesPath = System.getProperty("DAGGER_CONFIG_PATH");
        Properties properties = new Properties();
        try {
            FileReader reader=new FileReader(daggerPropertiesPath);
            properties.load(reader);
            this.environmentParameters.putAll(properties.entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(),
                            e -> e.getValue().toString())));

        } catch (Exception e) {
            e.printStackTrace();
            throw new DaggerConfigurationException("Config source not provided");
        }


        this.environmentParameters.entrySet().forEach( t -> System.out.println(t.getKey() + t.getValue()));
    }


    @Override
    public Configuration get() {
        return ParameterTool.fromMap(this.environmentParameters).getConfiguration();
    }

    private Map<String, String> environmentParameters;


}
