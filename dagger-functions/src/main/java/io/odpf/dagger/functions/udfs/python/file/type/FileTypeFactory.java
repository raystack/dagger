package io.odpf.dagger.functions.udfs.python.file.type;

import io.odpf.dagger.functions.exceptions.PythonFilesFormatException;

/**
 * The type File type factory.
 */
public class FileTypeFactory {

    private String pythonFile;

    /**
     * Instantiates a new File type factory.
     *
     * @param pythonFile the python file
     */
    public FileTypeFactory(String pythonFile) {
        this.pythonFile = pythonFile;
    }

    /**
     * Gets file type.
     *
     * @return the file type
     */
    public FileType getFileType() {
        switch (getFileTypeFormat()) {
            case "PY":
                return new PythonFileType(pythonFile);
            case "ZIP":
                return new ZipFileType(pythonFile);
            default:
                throw new PythonFilesFormatException("Python files should be in .py or .zip format");
        }
    }

    private String getFileTypeFormat() {
        String[] files = pythonFile.split("\\.");
        return files[files.length - 1].toUpperCase();
    }
}
