package io.odpf.dagger.functions.udfs.python.file.source;

import io.odpf.dagger.functions.udfs.python.file.source.local.LocalFileSource;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public class LocalFileSourceTest {

    @Test
    public void shouldGetObjectFile() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource("python_udf.zip")).getFile());
        String pathFile = file.getPath();

        byte[] object = Files.readAllBytes(Paths.get(pathFile));
        String stringObject = new String(object, StandardCharsets.UTF_8);

        LocalFileSource localFileSource = new LocalFileSource(pathFile);
        byte[] actualObject = localFileSource.getObjectFile();

        String actualStringObject = new String(actualObject, StandardCharsets.UTF_8);
        Assert.assertEquals(stringObject, actualStringObject);
    }
}
