package io.odpf.dagger.functions.udfs.python;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class PythonUdfManager {
    private static final String PYTHON_FILES = "python.files";

    private StreamTableEnvironment tableEnvironment;
    private PythonUdfConfigMapper pythonUdfConfigMapper;

    public PythonUdfManager(StreamTableEnvironment tableEnvironment, PythonUdfConfigMapper pythonUdfConfigMapper) {
        this.tableEnvironment = tableEnvironment;
        this.pythonUdfConfigMapper = pythonUdfConfigMapper;
    }

    private void registerPythonConfig(Map<String, Object> configMap) {
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            if (entry.getValue() instanceof Long) {
                tableEnvironment
                        .getConfig()
                        .getConfiguration()
                        .setLong(entry.getKey(), (Long) entry.getValue());
            } else if (entry.getValue() instanceof Boolean) {
                tableEnvironment
                        .getConfig()
                        .getConfiguration()
                        .setBoolean(entry.getKey(), (Boolean) entry.getValue());
            } else if (entry.getValue() instanceof Integer) {
                tableEnvironment
                        .getConfig()
                        .getConfiguration()
                        .setInteger(entry.getKey(), (Integer) entry.getValue());
            } else {
                tableEnvironment
                        .getConfig()
                        .getConfiguration()
                        .setString(entry.getKey(), (String) entry.getValue());
            }
        }
    }

    public void registerPythonFunctions() throws IOException {

        Map<String, Object> configMap = pythonUdfConfigMapper.getConfig();
        registerPythonConfig(configMap);
        String[] pythonFilesSource = ((String) configMap.get(PYTHON_FILES)).split(",");

        for (String pythonFile : pythonFilesSource) {
            if (pythonFile.contains(".zip")) {
                ZipFile zf = new ZipFile(pythonFile);
                for (Enumeration e = zf.entries(); e.hasMoreElements();) {
                    ZipEntry entry = (ZipEntry) e.nextElement();
                    String name = entry.getName();
                    if (name.endsWith(".py")) {
                        name = name.replace(".py", "").replace("/", ".");
                        String udfName = name.substring(name.lastIndexOf(".") + 1);
                        String query = "CREATE TEMPORARY FUNCTION " + udfName.toUpperCase() + " AS '" + name + "." + udfName + "' LANGUAGE PYTHON";
                        tableEnvironment.executeSql(query);
                    }
                }
            } else if (pythonFile.contains(".py")) {
                String name = pythonFile.substring(pythonFile.lastIndexOf('/') + 1).replace(".py", "");
                String query = "CREATE TEMPORARY FUNCTION " + name.toUpperCase() + " AS '" + name + "." + name + "' LANGUAGE PYTHON";
                tableEnvironment.executeSql(query);
            } else {
                throw new IOException("Python files should be in .py or .zip format");
            }
        }
    }
}
