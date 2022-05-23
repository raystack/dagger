package io.odpf.dagger.functions.udfs.python.file.type;

import io.odpf.dagger.functions.udfs.python.file.source.FileSourceFactory;
import io.odpf.dagger.functions.udfs.python.file.source.gcs.GcsClient;
import io.odpf.dagger.functions.udfs.python.file.source.gcs.GcsFileSource;
import io.odpf.dagger.functions.udfs.python.file.source.local.LocalFileSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ZipFileTypeTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private GcsClient gcsClient;

    @Mock
    private LocalFileSource localFileSource;

    @Mock
    private GcsFileSource gcsFileSource;

    @Mock
    private FileSourceFactory fileSourceFactory;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldGetFileNamesFromLocalZip() throws IOException {
        String pathFile = "/path/to/file/python_udf.zip";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource("python_udf.zip")).getFile());

        byte[] zipInBytes = Files.readAllBytes(file.toPath());

        when(fileSourceFactory.getFileSource()).thenReturn(localFileSource);
        when(localFileSource.getObjectFile()).thenReturn(zipInBytes);

        ZipFileType zipFileType = new ZipFileType(pathFile, fileSourceFactory);

        List<String> fileNames = zipFileType.getFileNames();

        Assert.assertEquals("[python_udf/scalar/add.py, python_udf/vectorized/substract.py]", fileNames.toString());
    }

    @Test
    public void shouldGetFileNamesFromGcsZip() throws IOException {
        String pathFile = "gs://bucket-name/path/to/file/python_udf.zip";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource("python_udf.zip")).getFile());

        byte[] zipInBytes = Files.readAllBytes(file.toPath());

        when(fileSourceFactory.getFileSource()).thenReturn(gcsFileSource);
        when(gcsFileSource.getGcsClient()).thenReturn(gcsClient);
        when(gcsFileSource.getObjectFile()).thenReturn(zipInBytes);

        ZipFileType zipFileType = new ZipFileType(pathFile, fileSourceFactory);

        List<String> fileNames = zipFileType.getFileNames();

        Assert.assertEquals("[python_udf/scalar/add.py, python_udf/vectorized/substract.py]", fileNames.toString());
    }
}
