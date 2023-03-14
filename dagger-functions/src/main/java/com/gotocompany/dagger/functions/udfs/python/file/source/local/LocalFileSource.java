package com.gotocompany.dagger.functions.udfs.python.file.source.local;

import com.gotocompany.dagger.functions.udfs.python.file.source.FileSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * The type Local file source.
 */
public class LocalFileSource implements FileSource {

    private String pythonFile;

    /**
     * Instantiates a new Local file source.
     *
     * @param pythonFile the python file
     */
    public LocalFileSource(String pythonFile) {
        this.pythonFile = pythonFile;
    }

    @Override
    public byte[] getObjectFile() throws IOException {
        return Files.readAllBytes(Paths.get(pythonFile));
    }
}
