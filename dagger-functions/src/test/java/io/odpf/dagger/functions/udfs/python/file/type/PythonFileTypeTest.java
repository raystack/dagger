package io.odpf.dagger.functions.udfs.python.file.type;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Objects;

public class PythonFileTypeTest {

    @Test
    public void shouldGetFileNames() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource("test_udf.py")).getFile());
        String pathFile = file.getAbsolutePath();

        PythonFileType pythonFileType = new PythonFileType(pathFile);
        List<String> fileNames = pythonFileType.getFileNames();

        Assert.assertEquals("[test_udf.py]", fileNames.toString());
    }
}
