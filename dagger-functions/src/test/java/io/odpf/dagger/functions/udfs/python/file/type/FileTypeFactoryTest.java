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
        String pythonFile = "/path/to/file/test_udf.py";

        FileType fileType = FileTypeFactory.getFileType(pythonFile);

        Assert.assertTrue(fileType instanceof PythonFileType);
    }

    @Test
    public void shouldGetZipFileType() {
        String pythonFile = "/path/to/file/python_udf.zip";

        FileType fileType = FileTypeFactory.getFileType(pythonFile);

        Assert.assertTrue(fileType instanceof ZipFileType);
    }

    @Test
    public void shouldThrowExceptionIfPythonFilesNotInZipOrPyFormat() {
        expectedEx.expect(PythonFilesFormatException.class);
        expectedEx.expectMessage("Python files should be in .py or .zip format");

        String pythonFile = "/path/to/file/test_file.txt";

        FileTypeFactory.getFileType(pythonFile);
    }
}
