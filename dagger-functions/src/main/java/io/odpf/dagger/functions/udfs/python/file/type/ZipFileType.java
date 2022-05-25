package io.odpf.dagger.functions.udfs.python.file.type;

import io.odpf.dagger.functions.udfs.python.file.source.FileSource;
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

    private FileSource fileSource;

    public ZipFileType(FileSource fileSource) {
        this.fileSource = fileSource;
    }

    @SneakyThrows
    @Override
    public List<String> getFileNames() {
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

    private boolean isPythonFile(String fileName) {
        return fileName.endsWith(".py");
    }
}
