package org.raystack.dagger.functions.udfs.python.file.type;

import org.raystack.dagger.functions.exceptions.PythonFilesFormatException;
import org.raystack.dagger.functions.udfs.python.file.source.FileSource;
import org.raystack.dagger.functions.udfs.python.file.source.FileSourceFactory;

/**
 * The type File type factory.
 */
public class FileTypeFactory {

    /**
     * Gets file type.
     *
     * @param pythonFile the python file
     * @return the file type
     */
    public static FileType getFileType(String pythonFile) {
        FileSource fileSource = FileSourceFactory.getFileSource(pythonFile);
        switch (getFileTypeFormat(pythonFile)) {
            case "PY":
                return new PythonFileType(pythonFile);
            case "ZIP":
                return new ZipFileType(fileSource);
            default:
                throw new PythonFilesFormatException("Python files should be in .py or .zip format");
        }
    }

    private static String getFileTypeFormat(String pythonFile) {
        String[] files = pythonFile.split("\\.");
        return files[files.length - 1].toUpperCase();
    }
}
