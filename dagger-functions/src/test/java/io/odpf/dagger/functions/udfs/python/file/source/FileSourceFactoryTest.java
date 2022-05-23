package io.odpf.dagger.functions.udfs.python.file.source;

import io.odpf.dagger.functions.udfs.python.file.source.gcs.GcsFileSource;
import io.odpf.dagger.functions.udfs.python.file.source.local.LocalFileSource;
import org.junit.Assert;
import org.junit.Test;

public class FileSourceFactoryTest {

    @Test
    public void shouldGetLocalFileSource() {
        String pythonFileSource = "/path/to/file/test_function.py";

        FileSourceFactory fileSourceFactory = new FileSourceFactory(pythonFileSource);

        Assert.assertTrue(fileSourceFactory.getFileSource() instanceof LocalFileSource);
    }

    @Test
    public void shouldGetGcsFileSource() {
        String pythonFileSource = "gs://bucket-name/path/to/file/test_function.py";

        FileSourceFactory fileSourceFactory = new FileSourceFactory(pythonFileSource);

        Assert.assertTrue(fileSourceFactory.getFileSource() instanceof GcsFileSource);
    }
}
