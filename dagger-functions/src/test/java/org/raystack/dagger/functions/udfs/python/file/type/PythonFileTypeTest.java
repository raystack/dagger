package org.raystack.dagger.functions.udfs.python.file.type;

import org.raystack.dagger.functions.exceptions.PythonFilesEmptyException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class PythonFileTypeTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void shouldGetFileNames() {
        ClassLoader classLoader = getClass().getClassLoader();
        String pythonFile = classLoader.getResource("test_udf.py").getPath();

        PythonFileType pythonFileType = new PythonFileType(pythonFile);
        List<String> fileNames = pythonFileType.getFileNames();

        Assert.assertEquals("[test_udf.py]", fileNames.toString());
    }

    @Test
    public void shouldGetEmptyFileNamesIfPythonFilesIsEmpty() {
        String pythonFile = "";

        PythonFileType pythonFileType = new PythonFileType(pythonFile);
        List<String> fileNames = pythonFileType.getFileNames();

        Assert.assertEquals("[]", fileNames.toString());
    }

    @Test
    public void shouldThrowNullPointerExceptionIfPythonFilesIsNull() {
        expectedEx.expect(PythonFilesEmptyException.class);
        expectedEx.expectMessage("Python files can not be null");

        PythonFileType pythonFileType = new PythonFileType(null);
        pythonFileType.getFileNames();
    }
}
