package io.odpf.dagger.functions.udfs.python.file.type;

import io.odpf.dagger.functions.udfs.python.file.source.FileSource;
import io.odpf.dagger.functions.udfs.python.file.source.FileSourceFactory;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * The type Zip file type.
 */
public class ZipFileType implements FileType {

    private String pythonFile;
    private FileSourceFactory fileSourceFactory;

    /**
     * Instantiates a new Zip file type.
     *
     * @param pythonFile the python file
     */
    public ZipFileType(String pythonFile) {
        this.pythonFile = pythonFile;
    }

    /**
     * Instantiates a new Zip file type.
     * This constructor used for unit test purposes.
     *
     * @param pythonFile  the python file
     * @param fileSourceFactory the file source factory
     */
    public ZipFileType(String pythonFile, FileSourceFactory fileSourceFactory) {
        this.pythonFile = pythonFile;
        this.fileSourceFactory = fileSourceFactory;
    }

    @SneakyThrows
    @Override
    public List<String> getFileNames() {
        FileSource fileSource = getFileSource();
        byte[] object = fileSource.getObjectFile();

        ZipInputStream zi = new ZipInputStream(new ByteArrayInputStream(object));
        ZipEntry zipEntry;
        List<ZipEntry> entries = new ArrayList<>();
        while ((zipEntry = zi.getNextEntry()) != null) {
            entries.add(zipEntry);
        }

        List<String> fileNames = new ArrayList<>();
        for (ZipEntry entry : entries) {
            String name = entry.getName();
            if (isPythonFile(name)) {
                fileNames.add(name);
            }
        }
        return fileNames;
    }

    public FileSource getFileSource() {
        if (fileSourceFactory == null) {
            fileSourceFactory = new FileSourceFactory(pythonFile);
        }
        return fileSourceFactory.getFileSource();
    }

    private boolean isPythonFile(String fileName) {
        return fileName.endsWith(".py");
    }
}
