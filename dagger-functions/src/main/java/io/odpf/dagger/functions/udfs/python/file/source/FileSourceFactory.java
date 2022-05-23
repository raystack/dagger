package io.odpf.dagger.functions.udfs.python.file.source;

import io.odpf.dagger.functions.udfs.python.file.source.gcs.GcsFileSource;
import io.odpf.dagger.functions.udfs.python.file.source.local.LocalFileSource;

/**
 * The type File source factory.
 */
public class FileSourceFactory {

    private String pythonFile;

    /**
     * Instantiates a new File source factory.
     *
     * @param pythonFile the python file
     */
    public FileSourceFactory(String pythonFile) {
        this.pythonFile = pythonFile;
    }

    /**
     * Gets file source.
     *
     * @return the file source
     */
    public FileSource getFileSource() {
        if ("GS".equals(getFileSourcePrefix())) {
            return new GcsFileSource(pythonFile);
        } else {
            return new LocalFileSource(pythonFile);
        }
    }

    private String getFileSourcePrefix() {
        String[] files = pythonFile.split("://");
        return files[0].toUpperCase();
    }
}
