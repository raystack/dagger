package io.odpf.dagger.functions.udfs.python;

import io.odpf.dagger.common.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static io.odpf.dagger.functions.common.Constants.PYTHON_UDF_CONFIG;
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
    public void shouldParsePythonUdfConfig() {
        when(configuration.getString(PYTHON_UDF_CONFIG, "")).thenReturn(pythonJsonConfig);
        PythonUdfConfig pythonUdfConfig = PythonUdfConfig.parse(configuration);

        Map<String, String> pythonUdfConfigMap = new LinkedHashMap<>();
        pythonUdfConfigMap.put("python.files", "/path/to/function.zip");
        pythonUdfConfigMap.put("python.requirements", "requirements.txt");
        pythonUdfConfigMap.put("python.archives", "/path/to/file.txt");
        pythonUdfConfigMap.put("python.client.executable", "python");
        pythonUdfConfigMap.put("python.executable", "python");
        pythonUdfConfigMap.put("python.fn-execution.arrow.batch.size", "1000");
        pythonUdfConfigMap.put("python.fn-execution.bundle.size", "10000");
        pythonUdfConfigMap.put("python.fn-execution.bundle.time", "1000");
        pythonUdfConfigMap.put("python.fn-execution.memory.managed", "true");
        pythonUdfConfigMap.put("python.map-state.iterate-response-batch-size", "1000");
        pythonUdfConfigMap.put("python.map-state.read-cache-size", "1000");
        pythonUdfConfigMap.put("python.map-state.write-cache-size", "1000");
        pythonUdfConfigMap.put("python.state-cache-size", "1000");
        pythonUdfConfigMap.put("python.metric.enabled", "true");
        pythonUdfConfigMap.put("python.profile.enabled", "false");

        assertMapEquals(pythonUdfConfigMap, pythonUdfConfig.getPythonParsedConfig());
    }

    private void assertMapEquals(Map<String, String> firstMap, Map<String, String> secondMap) {
        Assert.assertEquals(firstMap.size(), secondMap.size());
        Assert.assertEquals(firstMap, secondMap);
    }
}
