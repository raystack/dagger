package io.odpf.dagger.functions.udfs.python.file.source;

import io.odpf.dagger.functions.udfs.python.file.source.gcs.GcsClient;
import io.odpf.dagger.functions.udfs.python.file.source.gcs.GcsFileSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class GcsFileSourceTest {

    @Mock
    private GcsClient gcsClient;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldGetObjectFile() throws IOException {
        String pathFile = "gs://bucket-name/path/to/file/test_function.py";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource("python_udf.zip")).getFile());
        byte[] expectedObject = Files.readAllBytes(file.toPath());

        when(gcsClient.getFile(pathFile)).thenReturn(expectedObject);
        GcsFileSource a = new GcsFileSource(pathFile, gcsClient);

        byte[] actualObject = a.getObjectFile();

        Assert.assertEquals(expectedObject, actualObject);
    }
}
