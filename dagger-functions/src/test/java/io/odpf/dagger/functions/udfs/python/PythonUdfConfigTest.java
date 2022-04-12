package io.odpf.dagger.functions.udfs.python;

import io.odpf.dagger.common.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static io.odpf.dagger.functions.common.Constants.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PythonUdfConfigTest {

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    private String pythonJsonConfig = "{ \"PYTHON_FILES\": \"/path/to/function.zip\", \"PYTHON_ARCHIVES\": \"/path/to/file.txt\", \"PYTHON_CLIENT_EXECUTABLE\": \"python\", \"PYTHON_EXECUTABLE\": \"python\", \"PYTHON_REQUIREMENTS\": \"requirements.txt\", \"PYTHON_METRIC_ENABLED\": \"true\", \"PYTHON_PROFILE_ENABLED\": \"false\", \"PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE\": \"1000\", \"PYTHON_FN_EXECUTION_BUNDLE_SIZE\": \"10000\", \"PYTHON_FN_EXECUTION_BUNDLE_TIME\": \"1000\", \"PYTHON_FN_EXECUTION_MEMORY_MANAGED\": \"true\", \"PYTHON_MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE\": \"1000\", \"PYTHON_MAP_STATE_READ_CACHE_SIZE\": \"1000\", \"PYTHON_MAP_STATE_WRITE_CACHE_SIZE\": \"1000\", \"PYTHON_STATE_CACHE_SIZE\": \"1000\" }";

    @Test
    public void shouldParseConfig() {
        when(configuration.getString(PYTHON_UDF_CONFIG, "")).thenReturn(pythonJsonConfig);
        PythonUdfConfig pythonUdfConfig = PythonUdfConfig.parse(configuration);

        Assert.assertNotNull(pythonUdfConfig);
    }

    @Test
    public void shouldSetConfigurationsFromJsonPythonUdfConfig() {
        when(configuration.getString(PYTHON_UDF_CONFIG, "")).thenReturn(pythonJsonConfig);
        PythonUdfConfig pythonUdfConfig = PythonUdfConfig.parse(configuration);

        Assert.assertEquals("/path/to/file.txt", pythonUdfConfig.getPythonArchives());
        Assert.assertEquals("1000", pythonUdfConfig.getPythonArrowBatchSize());
        Assert.assertEquals("10000", pythonUdfConfig.getPythonBundleSize());
        Assert.assertEquals("1000", pythonUdfConfig.getPythonBundleTime());
        Assert.assertEquals("python", pythonUdfConfig.getPythonClientExecutable());
        Assert.assertEquals("python", pythonUdfConfig.getPythonExecutable());
        Assert.assertEquals("/path/to/function.zip", pythonUdfConfig.getPythonFiles());
        Assert.assertEquals("true", pythonUdfConfig.getPythonMemoryManaged());
        Assert.assertEquals("true", pythonUdfConfig.getPythonMetricEnabled());
        Assert.assertEquals("false", pythonUdfConfig.getPythonProfileEnabled());
        Assert.assertEquals("1000", pythonUdfConfig.getPythonReadCacheSize());
        Assert.assertEquals("requirements.txt", pythonUdfConfig.getPythonRequirements());
        Assert.assertEquals("1000", pythonUdfConfig.getPythonResponseBatchSize());
        Assert.assertEquals("1000", pythonUdfConfig.getPythonStateCacheSize());
        Assert.assertEquals("1000", pythonUdfConfig.getPythonWriteCacheSize());
    }

    @Test
    public void shouldMapJsonConfig() {
        when(configuration.getString(PYTHON_UDF_CONFIG, "")).thenReturn(pythonJsonConfig);
        PythonUdfConfig pythonUdfConfig = PythonUdfConfig.parse(configuration);

        Map<String, String> pythonUdfConfigMap = new HashMap<>();
        pythonUdfConfigMap.put(PYTHON_FILES_KEY, "/path/to/function.zip");
        pythonUdfConfigMap.put(PYTHON_REQUIREMENTS_KEY, "requirements.txt");
        pythonUdfConfigMap.put(PYTHON_ARCHIVES_KEY, "/path/to/file.txt");
        pythonUdfConfigMap.put(PYTHON_CLIENT_EXECUTABLE_KEY, "python");
        pythonUdfConfigMap.put(PYTHON_EXECUTABLE_KEY, "python");
        pythonUdfConfigMap.put(PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY, "1000");
        pythonUdfConfigMap.put(PYTHON_FN_EXECUTION_BUNDLE_SIZE_KEY, "10000");
        pythonUdfConfigMap.put(PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY, "1000");
        pythonUdfConfigMap.put(PYTHON_FN_EXECUTION_MEMORY_MANAGED_KEY, "true");
        pythonUdfConfigMap.put(PYTHON_MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE_KEY, "1000");
        pythonUdfConfigMap.put(PYTHON_MAP_STATE_READ_CACHE_SIZE_KEY, "1000");
        pythonUdfConfigMap.put(PYTHON_MAP_STATE_WRITE_CACHE_SIZE_KEY, "1000");
        pythonUdfConfigMap.put(PYTHON_STATE_CACHE_SIZE_KEY, "1000");
        pythonUdfConfigMap.put(PYTHON_METRIC_ENABLED_KEY, "true");
        pythonUdfConfigMap.put(PYTHON_PROFILE_ENABLED_KEY, "false");

        assertMapEquals(pythonUdfConfigMap, pythonUdfConfig.jsonToConfigMap());
    }

    private void assertMapEquals(Map<String, String> firstMap, Map<String, String> secondMap) {
        Assert.assertEquals(firstMap.size(), secondMap.size());
        Assert.assertEquals(firstMap, secondMap);
    }
}
