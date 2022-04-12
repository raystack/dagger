package io.odpf.dagger.functions.udfs.python;

import io.odpf.dagger.functions.utils.ConfigMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.odpf.dagger.functions.common.Constants.*;

public class PythonUdfConfigMapper implements ConfigMapper {

    private PythonUdfConfig pythonUdfConfig;

    private static final Map<String, String> KEY_MAP = createKeyMap();
    private static final Map<String, Class> TYPE_MAP = createTypeMap();

    public PythonUdfConfigMapper(PythonUdfConfig pythonUdfConfig) {
        this.pythonUdfConfig = pythonUdfConfig;
    }

    private static Map<String, String> createKeyMap() {
        Map<String, String> pythonKeyMap = new HashMap<>();
        pythonKeyMap.put(PYTHON_FILES_KEY, "python.files");
        pythonKeyMap.put(PYTHON_REQUIREMENTS_KEY, "python.requirements");
        pythonKeyMap.put(PYTHON_ARCHIVES_KEY, "python.archives");
        pythonKeyMap.put(PYTHON_CLIENT_EXECUTABLE_KEY, "python.client.executable");
        pythonKeyMap.put(PYTHON_EXECUTABLE_KEY, "python.executable");
        pythonKeyMap.put(PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY, "python.fn-execution.arrow.batch.size");
        pythonKeyMap.put(PYTHON_FN_EXECUTION_BUNDLE_SIZE_KEY, "python.fn-execution.bundle.size");
        pythonKeyMap.put(PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY, "python.fn-execution.bundle.time");
        pythonKeyMap.put(PYTHON_FN_EXECUTION_MEMORY_MANAGED_KEY, "python.fn-execution.memory.managed");
        pythonKeyMap.put(PYTHON_MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE_KEY, "python.map-state.iterate-response-batch-size");
        pythonKeyMap.put(PYTHON_MAP_STATE_READ_CACHE_SIZE_KEY, "python.map-state.read-cache-size");
        pythonKeyMap.put(PYTHON_MAP_STATE_WRITE_CACHE_SIZE_KEY, "python.map-state.write-cache-size");
        pythonKeyMap.put(PYTHON_STATE_CACHE_SIZE_KEY, "python.state.cache-size");
        pythonKeyMap.put(PYTHON_METRIC_ENABLED_KEY, "python.metric.enabled");
        pythonKeyMap.put(PYTHON_PROFILE_ENABLED_KEY, "python.profile.enabled");
        return Collections.unmodifiableMap(pythonKeyMap);
    }

    private static Map<String, Class> createTypeMap() {
        Map<String, Class> pythonTypeMap = new HashMap<>();
        pythonTypeMap.put(PYTHON_FILES_KEY, String.class);
        pythonTypeMap.put(PYTHON_REQUIREMENTS_KEY, String.class);
        pythonTypeMap.put(PYTHON_ARCHIVES_KEY, String.class);
        pythonTypeMap.put(PYTHON_CLIENT_EXECUTABLE_KEY, String.class);
        pythonTypeMap.put(PYTHON_EXECUTABLE_KEY, String.class);
        pythonTypeMap.put(PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY, Integer.class);
        pythonTypeMap.put(PYTHON_FN_EXECUTION_BUNDLE_SIZE_KEY, Integer.class);
        pythonTypeMap.put(PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY, Long.class);
        pythonTypeMap.put(PYTHON_FN_EXECUTION_MEMORY_MANAGED_KEY, Boolean.class);
        pythonTypeMap.put(PYTHON_MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE_KEY, Integer.class);
        pythonTypeMap.put(PYTHON_MAP_STATE_READ_CACHE_SIZE_KEY, Integer.class);
        pythonTypeMap.put(PYTHON_MAP_STATE_WRITE_CACHE_SIZE_KEY, Integer.class);
        pythonTypeMap.put(PYTHON_STATE_CACHE_SIZE_KEY, Integer.class);
        pythonTypeMap.put(PYTHON_METRIC_ENABLED_KEY, Boolean.class);
        pythonTypeMap.put(PYTHON_PROFILE_ENABLED_KEY, Boolean.class);

        return Collections.unmodifiableMap(pythonTypeMap);
    }

    public Map<String, Object> getConfig() {
        Map<String, String> configMap = pythonUdfConfig.jsonToConfigMap();

        Map<String, Object> config = new HashMap<>();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            if (getType(entry.getKey()) == Long.class) {
                config.put(getKey(entry.getKey()), Long.parseLong(getValue(configMap, entry.getKey())));
            } else if (getType(entry.getKey()) == Boolean.class) {
                config.put(getKey(entry.getKey()), Boolean.parseBoolean(getValue(configMap, entry.getKey())));
            } else if (getType(entry.getKey()) == Integer.class) {
                config.put(getKey(entry.getKey()), Integer.parseInt(getValue(configMap, entry.getKey())));
            } else {
                config.put(getKey(entry.getKey()), getValue(configMap, entry.getKey()));
            }
        }

        return config;
    }

    @Override
    public String getKey(String key) {
        return KEY_MAP.get(key);
    }

    @Override
    public String getValue(Map<String, String> inputMap, String key) {
        return inputMap.get(key);
    }

    @Override
    public Class getType(String key) {
        return TYPE_MAP.get(key);
    }
}
