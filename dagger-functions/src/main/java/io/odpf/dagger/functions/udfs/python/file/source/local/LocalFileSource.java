package io.odpf.dagger.functions.udfs.python.file.source.local;

import io.odpf.dagger.functions.udfs.python.file.source.FileSource;
import lombok.SneakyThrows;

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
     * @param pythonFileSource the python file
     */
    public LocalFileSource(String pythonFileSource) {
        this.pythonFile = pythonFileSource;
    }

    @SneakyThrows
    @Override
    public byte[] getObjectFile() {
        return Files.readAllBytes(Paths.get(pythonFile));
    }
}
