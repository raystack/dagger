package com.gotocompany.dagger.functions.udfs.python.file.source.local;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LocalFileSourceTest {

    @Test
    public void shouldGetObjectFile() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        String pythonFile = classLoader.getResource("python_udf.zip").getPath();

        byte[] object = Files.readAllBytes(Paths.get(pythonFile));
        String stringObject = new String(object, StandardCharsets.UTF_8);

        LocalFileSource localFileSource = new LocalFileSource(pythonFile);
        byte[] actualObject = localFileSource.getObjectFile();

        String actualStringObject = new String(actualObject, StandardCharsets.UTF_8);
        Assert.assertEquals(stringObject, actualStringObject);
    }
}
