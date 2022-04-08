package io.odpf.dagger.functions.udfs.python;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class PythonUdfManager {

    private StreamTableEnvironment tableEnvironment;
    private PythonUdfConfig pythonUdfConfig;

    public PythonUdfManager(StreamTableEnvironment tableEnvironment, PythonUdfConfig pythonUdfConfig) {
        this.tableEnvironment = tableEnvironment;
        this.pythonUdfConfig = pythonUdfConfig;
    }

    private void registerPythonUdfConfig() {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();

        Map<String, String> pythonParsedConfig = pythonUdfConfig.getPythonParsedConfig();
        for (Map.Entry<String, String> entry : pythonParsedConfig.entrySet()) {
            if (entry.getKey().contains("size")) {
                config.setInteger(entry.getKey(), Integer.parseInt(entry.getValue()));
            } else if (entry.getKey().contains("enabled") | entry.getKey().contains("managed")) {
                config.setBoolean(entry.getKey(), Boolean.parseBoolean(entry.getValue()));
            } else if (entry.getKey().contains("time")) {
                config.setLong(entry.getKey(), Long.parseLong(entry.getValue()));
            } else {
                config.setString(entry.getKey(), entry.getValue());
            }
        }
        tableEnvironment.getConfig().addConfiguration(config);
    }

    public void registerPythonFunctions() throws IOException {
        registerPythonUdfConfig();
        String[] pythonFilesSource = pythonUdfConfig
                .getPythonFiles()
                .split(",");

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
