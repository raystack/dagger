package io.odpf.dagger.functions.udfs.python.file.type;

import io.odpf.dagger.functions.udfs.python.file.source.gcs.GcsFileSource;
import io.odpf.dagger.functions.udfs.python.file.source.local.LocalFileSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ZipFileTypeTest {

    @Mock
    private GcsFileSource gcsFileSource;

    @Mock
    private LocalFileSource localFileSource;

    private byte[] zipInBytes;

    @Before
    public void setup() throws IOException {
        initMocks(this);
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource("python_udf.zip")).getFile());
        zipInBytes = Files.readAllBytes(file.toPath());
    }

    @Test
    public void shouldGetFileNamesFromLocalZip() throws IOException {

        when(localFileSource.getObjectFile()).thenReturn(zipInBytes);

        ZipFileType zipFileType = new ZipFileType(localFileSource);
        List<String> fileNames = zipFileType.getFileNames();

        Assert.assertEquals("[python_udf/scalar/add.py, python_udf/vectorized/substract.py]", fileNames.toString());
    }

    @Test
    public void shouldGetFileNamesFromGcsZip() throws IOException {

        when(gcsFileSource.getObjectFile()).thenReturn(zipInBytes);

        ZipFileType zipFileType = new ZipFileType(gcsFileSource);
        List<String> fileNames = zipFileType.getFileNames();

        Assert.assertEquals("[python_udf/scalar/add.py, python_udf/vectorized/substract.py]", fileNames.toString());
    }

    @Test
    public void shouldGetEmptyFileNamesIfZipFileNotContainPyFile() throws IOException {

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource("test_no_py.zip")).getFile());
        zipInBytes = Files.readAllBytes(file.toPath());

        when(gcsFileSource.getObjectFile()).thenReturn(zipInBytes);

        ZipFileType zipFileType = new ZipFileType(gcsFileSource);
        List<String> fileNames = zipFileType.getFileNames();

        Assert.assertEquals("[]", fileNames.toString());
    }
}
