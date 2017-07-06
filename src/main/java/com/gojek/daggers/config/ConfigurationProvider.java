package com.gojek.daggers.config;

import java.util.Map;

public interface ConfigurationProvider {
    Map<String, String> get(String input);
}
