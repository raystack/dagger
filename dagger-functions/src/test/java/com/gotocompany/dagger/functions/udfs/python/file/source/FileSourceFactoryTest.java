package com.gotocompany.dagger.functions.udfs.python.file.source;

import com.gotocompany.dagger.functions.udfs.python.file.source.gcs.GcsFileSource;
import com.gotocompany.dagger.functions.udfs.python.file.source.local.LocalFileSource;
import org.junit.Assert;
import org.junit.Test;

public class FileSourceFactoryTest {

    @Test
    public void shouldGetLocalFileSource() {
        String pythonFile = "/path/to/file/test_function.py";

        FileSource fileSource = FileSourceFactory.getFileSource(pythonFile);

        Assert.assertTrue(fileSource instanceof LocalFileSource);
    }

    @Test
    public void shouldGetGcsFileSource() {
        String pythonFile = "gs://bucket-name/path/to/file/test_function.py";

        FileSource fileSource = FileSourceFactory.getFileSource(pythonFile);

        Assert.assertTrue(fileSource instanceof GcsFileSource);
    }
}
