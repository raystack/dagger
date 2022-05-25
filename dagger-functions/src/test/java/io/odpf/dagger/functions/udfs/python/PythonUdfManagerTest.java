package io.odpf.dagger.functions.udfs.python;

import io.odpf.dagger.functions.exceptions.PythonFilesFormatException;
import io.odpf.dagger.functions.exceptions.PythonFilesNullException;
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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.mockito.MockitoAnnotations.initMocks;

public class PythonUdfManagerTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Mock
    private StreamTableEnvironment tableEnvironment;

    @Mock
    private PythonUdfConfig pythonUdfConfig;

    @Mock
    private TableConfig tableConfig;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldRegisterPythonUdfConfig() {
        String pathFile = getPath("python_udf.zip");

        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(tableConfig.getConfiguration()).thenReturn(configuration);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(pathFile);
        when(pythonUdfConfig.getPythonArchives()).thenReturn("/path/to/file.txt");
        when(pythonUdfConfig.getPythonRequirements()).thenReturn("requirements.txt");
        when(pythonUdfConfig.getPythonArrowBatchSize()).thenReturn(10000);
        when(pythonUdfConfig.getPythonBundleSize()).thenReturn(100000);
        when(pythonUdfConfig.getPythonBundleTime()).thenReturn(1000L);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();

        verify(configuration, times(1)).setString("python.files", pathFile);
        verify(configuration, times(1)).setString("python.archives", "/path/to/file.txt");
        verify(configuration, times(1)).setString("python.requirements", "requirements.txt");
        verify(configuration, times(1)).setInteger("python.fn-execution.arrow.batch.size", 10000);
        verify(configuration, times(1)).setInteger("python.fn-execution.bundle.size", 100000);
        verify(configuration, times(1)).setLong("python.fn-execution.bundle.time", 1000);
    }

    @Test
    public void shouldNotRegisterConfigIfNotSet() {
        String pathFile = getPath("python_udf.zip");

        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(tableConfig.getConfiguration()).thenReturn(configuration);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(pathFile);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();

        verify(configuration, times(1)).setString("python.files", pathFile);
        verify(configuration, times(0)).setString("python.archives", "/path/to/file.txt");
        verify(configuration, times(0)).setString("python.requirements", "requirements.txt");
    }

    @Test
    public void shouldRegisterPythonUdfFromPyFile() {
        String pathFile = getPath("test_udf.py");
        String sqlRegisterUdf = "CREATE TEMPORARY FUNCTION TEST_UDF AS 'test_udf.test_udf' LANGUAGE PYTHON";

        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(tableConfig.getConfiguration()).thenReturn(configuration);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(pathFile);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();

        verify(configuration, times(1)).setString("python.files", pathFile);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterUdf);
    }

    @Test
    public void shouldOnlyExecutePyFormatInsideZipFile() {
        String pathFile = getPath("python_udf.zip");

        String sqlRegisterFirstUdf = "CREATE TEMPORARY FUNCTION TEST_FUNCTION AS 'python_udf.scalar.multiply.multiply' LANGUAGE PYTHON";
        String sqlRegisterSecondUdf = "CREATE TEMPORARY FUNCTION ADD AS 'python_udf.scalar.add.add' LANGUAGE PYTHON";
        String sqlRegisterThirdUdf = "CREATE TEMPORARY FUNCTION SUBSTRACT AS 'python_udf.vectorized.substract.substract' LANGUAGE PYTHON";

        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(tableConfig.getConfiguration()).thenReturn(configuration);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(pathFile);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();

        verify(configuration, times(1)).setString("python.files", pathFile);
        verify(tableEnvironment, times(0)).executeSql(sqlRegisterFirstUdf);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterSecondUdf);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterThirdUdf);
    }

    @Test
    public void shouldRegisterPythonUdfFromPyAndZipFile() {
        String zipPathFile = getPath("python_udf.zip");
        String pyPathFile = getPath("test_udf.py");

        String sqlRegisterFirstUdf = "CREATE TEMPORARY FUNCTION ADD AS 'python_udf.scalar.add.add' LANGUAGE PYTHON";
        String sqlRegisterSecondUdf = "CREATE TEMPORARY FUNCTION SUBSTRACT AS 'python_udf.vectorized.substract.substract' LANGUAGE PYTHON";
        String sqlRegisterThirdUdf = "CREATE TEMPORARY FUNCTION TEST_UDF AS 'test_udf.test_udf' LANGUAGE PYTHON";

        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(tableConfig.getConfiguration()).thenReturn(configuration);
        when(pythonUdfConfig.getPythonFiles()).thenReturn(zipPathFile + "," + pyPathFile);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();

        verify(configuration, times(1)).setString("python.files", zipPathFile + "," + pyPathFile);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterFirstUdf);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterSecondUdf);
        verify(tableEnvironment, times(1)).executeSql(sqlRegisterThirdUdf);
    }

    @Test
    public void shouldThrowExceptionIfPythonFilesNotInZipOrPyFormat() throws IOException {
        expectedEx.expect(PythonFilesFormatException.class);
        expectedEx.expectMessage("Python files should be in .py or .zip format");

        File file = File.createTempFile("test_file", ".txt");
        file.deleteOnExit();

        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(tableConfig.getConfiguration()).thenReturn(configuration);
        when(pythonUdfConfig.getPythonFiles()).thenReturn("test_file.txt");

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();
    }

    @Test
    public void shouldThrowExceptionIfPythonFilesIsNullOrEmpty() {
        expectedEx.expect(PythonFilesNullException.class);
        expectedEx.expectMessage("Python files can not be null");

        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(tableConfig.getConfiguration()).thenReturn(configuration);

        PythonUdfManager pythonUdfManager = new PythonUdfManager(tableEnvironment, pythonUdfConfig);
        pythonUdfManager.registerPythonFunctions();
    }

    private String getPath(String filename) {
        ClassLoader classLoader = getClass().getClassLoader();

        return classLoader.getResource(filename).getPath();
    }
}
