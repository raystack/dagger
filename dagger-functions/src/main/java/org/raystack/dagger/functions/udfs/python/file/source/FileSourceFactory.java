package org.raystack.dagger.functions.udfs.python.file.source;

import org.raystack.dagger.functions.udfs.python.file.source.gcs.GcsFileSource;
import org.raystack.dagger.functions.udfs.python.file.source.local.LocalFileSource;

/**
 * The type File source factory.
 */
public class FileSourceFactory {

    /**
     * Gets file source.
     *
     * @param pythonFile the python file
     * @return the file source
     */
    public static FileSource getFileSource(String pythonFile) {
        if ("GS".equals(getFileSourcePrefix(pythonFile))) {
            return new GcsFileSource(pythonFile);
        } else {
            return new LocalFileSource(pythonFile);
        }
    }

    private static String getFileSourcePrefix(String pythonFile) {
        String[] files = pythonFile.split("://");
        return files[0].toUpperCase();
    }
}
