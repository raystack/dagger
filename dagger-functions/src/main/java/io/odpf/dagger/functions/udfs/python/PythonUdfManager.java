package io.odpf.dagger.functions.udfs.python;

import io.odpf.dagger.functions.exceptions.PythonFilesNullException;
import io.odpf.dagger.functions.udfs.python.file.type.FileType;
import io.odpf.dagger.functions.udfs.python.file.type.FileTypeFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Python udf manager.
 */
public class PythonUdfManager {

    private StreamTableEnvironment tableEnvironment;
    private PythonUdfConfig pythonUdfConfig;

    /**
     * Instantiates a new Python udf manager.
     *
     * @param tableEnvironment the table environment
     * @param pythonUdfConfig  the python udf config
     */
    public PythonUdfManager(StreamTableEnvironment tableEnvironment, PythonUdfConfig pythonUdfConfig) {
        this.tableEnvironment = tableEnvironment;
        this.pythonUdfConfig = pythonUdfConfig;
    }

    /**
     * Register python functions.
     */
    public void registerPythonFunctions() {
        String inputFiles = pythonUdfConfig.getPythonFiles();
        String[] pythonFiles;
        if (inputFiles != null) {
            registerPythonConfig();
            pythonFiles = inputFiles.split(",");
        } else {
            throw new PythonFilesNullException("Python files can not be null");
        }

        for (String pythonFile : pythonFiles) {
            FileTypeFactory fileTypeFactory = new FileTypeFactory(pythonFile);
            FileType fileType = fileTypeFactory.getFileType();
            List<String> fileNames = fileType.getFileNames();
            List<String> sqlQueries = createQuery(fileNames);
            executeSql(sqlQueries);
        }
    }

    private void registerPythonConfig() {
        if (pythonUdfConfig.getPythonRequirements() != null) {
            tableEnvironment.getConfig().getConfiguration().setString("python.requirements", pythonUdfConfig.getPythonRequirements());
        }
        if (pythonUdfConfig.getPythonArchives() != null) {
            tableEnvironment.getConfig().getConfiguration().setString("python.archives", pythonUdfConfig.getPythonArchives());
        }
        tableEnvironment.getConfig().getConfiguration().setString("python.files", pythonUdfConfig.getPythonFiles());
        tableEnvironment.getConfig().getConfiguration().setInteger("python.fn-execution.arrow.batch.size", pythonUdfConfig.getPythonArrowBatchSize());
        tableEnvironment.getConfig().getConfiguration().setInteger("python.fn-execution.bundle.size", pythonUdfConfig.getPythonBundleSize());
        tableEnvironment.getConfig().getConfiguration().setLong("python.fn-execution.bundle.time", pythonUdfConfig.getPythonBundleTime());
    }

    private void executeSql(List<String> sqlQueries) {
        for (String query : sqlQueries) {
            tableEnvironment.executeSql(query);
        }
    }

    private List<String> createQuery(List<String> fileNames) {
        List<String> sqlQueries = new ArrayList<>();
        for (String fileName : fileNames) {
            fileName = fileName.replace(".py", "").replace("/", ".");
            String functionName = fileName.substring(fileName.lastIndexOf(".") + 1);
            String query = "CREATE TEMPORARY FUNCTION " + functionName.toUpperCase() + " AS '" + fileName + "." + functionName + "' LANGUAGE PYTHON";
            sqlQueries.add(query);
        }
        return sqlQueries;
    }
}
