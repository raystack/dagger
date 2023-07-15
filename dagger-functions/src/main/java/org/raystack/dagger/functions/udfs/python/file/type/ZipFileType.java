package org.raystack.dagger.functions.udfs.python.file.type;

import org.raystack.dagger.functions.udfs.python.file.source.FileSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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

    @Override
    public List<String> getFileNames() throws IOException {
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
