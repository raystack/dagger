package com.gotocompany.dagger.functions.udfs.python.file.type;

import com.gotocompany.dagger.functions.exceptions.PythonFilesEmptyException;

import java.util.Collections;
import java.util.List;

/**
 * The type Python file type.
 */
public class PythonFileType implements FileType {

    private String pythonFile;

    /**
     * Instantiates a new Python file type.
     *
     * @param pythonFile the python file
     */
    public PythonFileType(String pythonFile) {
        this.pythonFile = pythonFile;
    }

    @Override
    public List<String> getFileNames() {
        if (pythonFile == null) {
            throw new PythonFilesEmptyException("Python files can not be null");
        }
        String name = pythonFile.substring(pythonFile.lastIndexOf('/') + 1);

        return Collections.singletonList(name);
    }
}
