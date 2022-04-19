package io.odpf.dagger.functions.udfs.python;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class PythonUdfManager {

    private StreamTableEnvironment tableEnvironment;
    private PythonUdfConfig pythonUdfConfig;

    public PythonUdfManager(StreamTableEnvironment tableEnvironment, PythonUdfConfig pythonUdfConfig) {
        this.tableEnvironment = tableEnvironment;
        this.pythonUdfConfig = pythonUdfConfig;
    }

    public void registerPythonFunctions() throws IOException {

        registerPythonConfig();
        String[] pythonFilesSource = pythonUdfConfig.getPythonFiles().split(",");

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
}
