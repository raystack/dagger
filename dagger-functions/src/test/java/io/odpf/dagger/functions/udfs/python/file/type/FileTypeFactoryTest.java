package io.odpf.dagger.functions.udfs.python.file.type;

import io.odpf.dagger.functions.exceptions.PythonFilesFormatException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FileTypeFactoryTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void shouldGetPythonFileType() {
        String pythonFileSource = "/path/to/file/test_udf.py";

        FileTypeFactory fileTypeFactory = new FileTypeFactory(pythonFileSource);

        Assert.assertTrue(fileTypeFactory.getFileType() instanceof PythonFileType);
    }

    @Test
    public void shouldGetZipFileType() {
        String pythonFileSource = "/path/to/file/python_udf.zip";

        FileTypeFactory fileTypeFactory = new FileTypeFactory(pythonFileSource);

        Assert.assertTrue(fileTypeFactory.getFileType() instanceof ZipFileType);
    }

    @Test
    public void shouldThrowExceptionIfPythonFilesNotInZipOrPyFormat() {
        expectedEx.expect(PythonFilesFormatException.class);
        expectedEx.expectMessage("Python files should be in .py or .zip format");

        String pythonFileSource = "/path/to/file/test_file.txt";

        FileTypeFactory fileTypeFactory = new FileTypeFactory(pythonFileSource);
        fileTypeFactory.getFileType();
    }
}
