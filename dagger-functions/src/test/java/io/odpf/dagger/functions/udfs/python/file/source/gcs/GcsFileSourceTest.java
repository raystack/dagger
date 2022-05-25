package io.odpf.dagger.functions.udfs.python.file.source.gcs;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

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
        ClassLoader classLoader = getClass().getClassLoader();
        String pythonFile = classLoader.getResource("python_udf.zip").getFile();
        byte[] expectedObject = Files.readAllBytes(Paths.get(pythonFile));

        when(gcsClient.getFile(pythonFile)).thenReturn(expectedObject);
        GcsFileSource gcsFileSource = new GcsFileSource(pythonFile, gcsClient);

        byte[] actualObject = gcsFileSource.getObjectFile();

        Assert.assertEquals(expectedObject, actualObject);
    }
}
