package io.odpf.dagger.functions.udfs.python;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static io.odpf.dagger.functions.common.Constants.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PythonUdfConfigMapperTest {

    @Mock
    private PythonUdfConfig pythonUdfConfig;

    private Map<String, String> pythonUdfConfigMap;

    @Before
    public void setup() {
        initMocks(this);
        pythonUdfConfigMap = new HashMap<>();
        pythonUdfConfigMap.put(PYTHON_FILES_KEY, "/path/to/function.zip");
        pythonUdfConfigMap.put(PYTHON_REQUIREMENTS_KEY, "requirements.txt");
        pythonUdfConfigMap.put(PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY, "1000");
        pythonUdfConfigMap.put(PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY, "1000");
        pythonUdfConfigMap.put(PYTHON_METRIC_ENABLED_KEY, "true");
    }

    @Test
    public void shouldGetActualKey() {
        PythonUdfConfigMapper pythonUdfConfigMapper = new PythonUdfConfigMapper(pythonUdfConfig);

        when(pythonUdfConfig.jsonToConfigMap()).thenReturn(pythonUdfConfigMap);

        Assert.assertEquals("python.files", pythonUdfConfigMapper.getKey(PYTHON_FILES_KEY));
        Assert.assertEquals("python.requirements", pythonUdfConfigMapper.getKey(PYTHON_REQUIREMENTS_KEY));
        Assert.assertEquals("python.fn-execution.arrow.batch.size", pythonUdfConfigMapper.getKey(PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY));
        Assert.assertEquals("python.fn-execution.bundle.time", pythonUdfConfigMapper.getKey(PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY));
        Assert.assertEquals("python.metric.enabled", pythonUdfConfigMapper.getKey(PYTHON_METRIC_ENABLED_KEY));
    }

    @Test
    public void shouldGetValue() {
        PythonUdfConfigMapper pythonUdfConfigMapper = new PythonUdfConfigMapper(pythonUdfConfig);

        when(pythonUdfConfig.jsonToConfigMap()).thenReturn(pythonUdfConfigMap);

        Assert.assertEquals("/path/to/function.zip", pythonUdfConfigMapper.getValue(pythonUdfConfigMap, PYTHON_FILES_KEY));
        Assert.assertEquals("requirements.txt", pythonUdfConfigMapper.getValue(pythonUdfConfigMap, PYTHON_REQUIREMENTS_KEY));
        Assert.assertEquals("1000", pythonUdfConfigMapper.getValue(pythonUdfConfigMap, PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY));
        Assert.assertEquals("1000", pythonUdfConfigMapper.getValue(pythonUdfConfigMap, PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY));
        Assert.assertEquals("true", pythonUdfConfigMapper.getValue(pythonUdfConfigMap, PYTHON_METRIC_ENABLED_KEY));
    }

    @Test
    public void shouldGetType() {
        PythonUdfConfigMapper pythonUdfConfigMapper = new PythonUdfConfigMapper(pythonUdfConfig);

        when(pythonUdfConfig.jsonToConfigMap()).thenReturn(pythonUdfConfigMap);

        Assert.assertEquals(String.class, pythonUdfConfigMapper.getType(PYTHON_FILES_KEY));
        Assert.assertEquals(String.class, pythonUdfConfigMapper.getType(PYTHON_REQUIREMENTS_KEY));
        Assert.assertEquals(Integer.class, pythonUdfConfigMapper.getType(PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY));
        Assert.assertEquals(Long.class, pythonUdfConfigMapper.getType(PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY));
        Assert.assertEquals(Boolean.class, pythonUdfConfigMapper.getType(PYTHON_METRIC_ENABLED_KEY));
    }

    @Test
    public void shouldGetConfig() {
        PythonUdfConfigMapper pythonUdfConfigMapper = new PythonUdfConfigMapper(pythonUdfConfig);

        Map<String, Object> config = new HashMap<>();
        config.put("python.files", "/path/to/function.zip");
        config.put("python.requirements", "requirements.txt");
        config.put("python.fn-execution.arrow.batch.size", 1000);
        config.put("python.fn-execution.bundle.time", 1000L);
        config.put("python.metric.enabled", true);

        when(pythonUdfConfig.jsonToConfigMap()).thenReturn(pythonUdfConfigMap);

        Assert.assertEquals(config, pythonUdfConfigMapper.getConfig());
    }
}
