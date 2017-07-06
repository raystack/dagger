package com.gojek.daggers.config;

import java.util.Map;

public class Configurations {

    public static Map<String, String> from(String configurationProviderClass, String input) {

        ConfigurationProvider configurationProvider;
        try {
            configurationProvider = (ConfigurationProvider) Class.forName(configurationProviderClass).getConstructors()[0].newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ConfigurationException("Bad configuration for provider class", e);
        }

        return configurationProvider.get(input);
    }
}
