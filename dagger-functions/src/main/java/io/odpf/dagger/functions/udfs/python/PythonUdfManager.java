package io.odpf.dagger.functions.udfs.python;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.odpf.dagger.functions.exceptions.PythonFilesFormatException;
import io.odpf.dagger.functions.exceptions.PythonFilesNotFoundException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class PythonUdfManager {

    private StreamTableEnvironment tableEnvironment;
    private PythonUdfConfig pythonUdfConfig;

    public PythonUdfManager(StreamTableEnvironment tableEnvironment, PythonUdfConfig pythonUdfConfig) {
        this.tableEnvironment = tableEnvironment;
        this.pythonUdfConfig = pythonUdfConfig;
    }

    public void registerPythonFunctions() throws IOException {

        String inputFiles = pythonUdfConfig.getPythonFiles();
        String[] pythonFilesSource;
        if (inputFiles != null) {
            registerPythonConfig();
            pythonFilesSource = inputFiles.split(",");
        } else {
            throw new PythonFilesNotFoundException("Python files not found");
        }

        for (String pythonFile : pythonFilesSource) {
            if (pythonFile.contains(".zip") && pythonFile.contains("gs://")) {
                registerFunctionFromGCSZipFile(pythonFile);
            } else if (pythonFile.contains(".zip")) {
                registerFunctionFromPathFile(pythonFile);
            } else if (pythonFile.contains(".py")) {
                registerFunctionFromPythonFile(pythonFile);
            } else {
                throw new PythonFilesFormatException("Python files should be in .py or .zip format");
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

    private String getBucketName(String fileSource) {
        String[] file = fileSource.replace("gs://", "").split("/");
        return file[0];
    }

    private String getObjectName(String fileSource) {
        return fileSource.substring(fileSource.lastIndexOf("/") + 1);
    }

    private void registerFunctionFromGCSZipFile(String fileSource) throws IOException {
        Storage storage = StorageOptions.newBuilder().build().getService();

        Blob blob = storage.get(BlobId.of(getBucketName(fileSource), getObjectName(fileSource)));
        List<ZipEntry> entries = new ArrayList<>();

        ZipInputStream zi = new ZipInputStream(new ByteArrayInputStream(blob.getContent()));
        ZipEntry zipEntry;
        while ((zipEntry = zi.getNextEntry()) != null) {
            entries.add(zipEntry);
        }

        for (ZipEntry entry : entries) {
            String name = entry.getName();
            execute(name);
        }
    }

    private void registerFunctionFromPathFile(String fileSource) throws IOException {
        ZipFile zipFile = new ZipFile(fileSource);
        Enumeration enumeration = zipFile.entries();
        while (enumeration.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) enumeration.nextElement();
            String name = entry.getName();
            execute(name);
        }
    }

    private void registerFunctionFromPythonFile(String fileSource) {
        String name = fileSource.substring(fileSource.lastIndexOf('/') + 1);
        execute(name);
    }

    private void execute(String name) {
        if (name.endsWith(".py")) {
            name = name.replace(".py", "").replace("/", ".");
            String functionName = name.substring(name.lastIndexOf(".") + 1);
            String query = "CREATE TEMPORARY FUNCTION " + functionName.toUpperCase() + " AS '" + name + "." + functionName + "' LANGUAGE PYTHON";
            tableEnvironment.executeSql(query);
        }
    }
}
