package io.odpf.dagger.functions.utils;

import java.util.Map;

public interface ConfigMapper {

    String getKey(String key);

    Object getValue(Map<String, String> inputMap, String key);

    Class getType(String key);
}
