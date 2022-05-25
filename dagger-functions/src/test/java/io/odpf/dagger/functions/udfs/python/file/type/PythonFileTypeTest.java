package io.odpf.dagger.functions.udfs.python.file.type;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PythonFileTypeTest {

    @Test
    public void shouldGetFileNames() {
        ClassLoader classLoader = getClass().getClassLoader();
        String pythonFile = classLoader.getResource("test_udf.py").getPath();

        PythonFileType pythonFileType = new PythonFileType(pythonFile);
        List<String> fileNames = pythonFileType.getFileNames();

        Assert.assertEquals("[test_udf.py]", fileNames.toString());
    }
}
