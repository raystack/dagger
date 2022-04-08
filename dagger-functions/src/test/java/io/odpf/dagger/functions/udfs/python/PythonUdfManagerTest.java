package io.odpf.dagger.functions.udfs.python;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.mockito.MockitoAnnotations.initMocks;

public class PythonUdfManagerTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Mock
    private PythonUdfConfig pythonUdfConfig;

    @Mock
    private StreamTableEnvironment tableEnvironment;

    @Mock
    private TableConfig tableConfig;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldRegisterPythonUdfConfig() throws IOException {
        String pathFile = getPath("python_udf.zip");
        String sqlRegisterFirstUdf = "CREATE TEMPORARY FUNCTION ADD AS 'python_udf.scalar.add.add' LANGUAGE PYTHON";
        String sqlRegisterSecondUdf = "CREATE TEMPORARY FUNCTION SUBSTRACT AS 'python_udf.vectorized.substract.substract' LANGUAGE PYTHON";

        Map<String, String> pythonUdfConfigMap = new LinkedHashMap<>();
        pythonUdfConfigMap.put("python.files", pathFile);
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

        when(pythonUdfConfig.getPythonParsedConfig()).thenReturn(pythonUdfConfigMap);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(pathFile);
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();

        verify(tableConfig, times(1)).addConfiguration(any(Configuration.class));
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterFirstUdf);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterSecondUdf);
    }

    @Test
    public void shouldRegisterPythonUdfFromPyFile() throws IOException {
        String pathFile = getPath("test_udf.py");
        String sqlRegisterUdf = "CREATE TEMPORARY FUNCTION TEST_UDF AS 'test_udf.test_udf' LANGUAGE PYTHON";

        Map<String, String> pythonUdfConfigMap = new LinkedHashMap<>();
        pythonUdfConfigMap.put("python.files", pathFile);

        when(pythonUdfConfig.getPythonParsedConfig()).thenReturn(pythonUdfConfigMap);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(pathFile);
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();

        verify(tableConfig, times(1)).addConfiguration(any(Configuration.class));
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterUdf);
    }

    @Test
    public void shouldRegisterPythonUdfFromPyAndZipFile() throws IOException {
        String zipPathFile = getPath("python_udf.zip");
        String pyPathFile = getPath("test_udf.py");

        String sqlRegisterFirstUdf = "CREATE TEMPORARY FUNCTION ADD AS 'python_udf.scalar.add.add' LANGUAGE PYTHON";
        String sqlRegisterSecondUdf = "CREATE TEMPORARY FUNCTION SUBSTRACT AS 'python_udf.vectorized.substract.substract' LANGUAGE PYTHON";
        String sqlRegisterThirdUdf = "CREATE TEMPORARY FUNCTION TEST_UDF AS 'test_udf.test_udf' LANGUAGE PYTHON";

        Map<String, String> pythonUdfConfigMap = new LinkedHashMap<>();
        pythonUdfConfigMap.put("python.files", zipPathFile + ", " + pyPathFile);

        when(pythonUdfConfig.getPythonParsedConfig()).thenReturn(pythonUdfConfigMap);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(zipPathFile + ", " + pyPathFile);
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();

        verify(tableConfig, times(1)).addConfiguration(any(Configuration.class));
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterFirstUdf);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterSecondUdf);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterThirdUdf);
    }

    @Test
    public void shouldThrowIOExceptionIfPythonFilesNotInZipOrPyFormat() throws IOException {
        expectedEx.expect(IOException.class);
        expectedEx.expectMessage("Python files should be in .py or .zip format");

        File file = File.createTempFile("test_file", ".txt");
        file.deleteOnExit();

        Map<String, String> pythonUdfConfigMap = new LinkedHashMap<>();
        pythonUdfConfigMap.put("python.files", file.getPath());

        when(pythonUdfConfig.getPythonParsedConfig()).thenReturn(pythonUdfConfigMap);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(file.getPath());
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();
    }

    @Test
    public void shouldThrowNullPointerExceptionIfPythonFilesNotExist() throws IOException {
        expectedEx.expect(NullPointerException.class);

        String pathFile = getPath("test_file.txt");

        Map<String, String> pythonUdfConfigMap = new LinkedHashMap<>();
        pythonUdfConfigMap.put("python.files", pathFile);

        when(pythonUdfConfig.getPythonParsedConfig()).thenReturn(pythonUdfConfigMap);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(pathFile);
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();
    }

    private String getPath(String filename) {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(filename)).getFile());
        return file.getAbsolutePath();
    }
}
