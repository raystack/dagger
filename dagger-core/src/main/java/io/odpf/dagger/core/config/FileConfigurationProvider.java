package io.odpf.dagger.core.config;

import org.apache.flink.api.java.utils.ParameterTool;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.exception.DaggerConfigurationException;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * The class which handle configuration provided from File..
 */
public class FileConfigurationProvider implements ConfigurationProvider {

    /**
     * Instantiates a new File configuration provider.
     */
    public FileConfigurationProvider() {
        this.environmentParameters = new HashMap<>();

        String daggerPropertiesPath = System.getProperty("DAGGER_CONFIG_PATH");
        Properties properties = new Properties();
        try {
            FileReader reader = new FileReader(daggerPropertiesPath);
            properties.load(reader);
            this.environmentParameters.putAll(properties.entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(),
                            e -> e.getValue().toString())));

        } catch (Exception e) {
            e.printStackTrace();
            throw new DaggerConfigurationException("Config source not provided");
        }


        this.environmentParameters.entrySet().forEach(t -> System.out.println(t.getKey() + t.getValue()));
    }

    @Override
    public Configuration get() {
        return new Configuration(ParameterTool.fromMap(this.environmentParameters));
    }

    private Map<String, String> environmentParameters;

}
