package org.raystack.dagger.functions.udfs.python.file.source.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class GcsClientTest {

    @Mock
    private Storage storage;

    @Mock
    private Blob blob;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldGetObjectFile() {

        String pythonFile = "gs://bucket_name/path/to/file/python_udf.zip";
        String bucketName = "bucket_name";
        String objectName = "path/to/file/python_udf.zip";
        String expectedValue = Arrays.toString("objectFile".getBytes());

        when(storage.get(BlobId.of(bucketName, objectName))).thenReturn(blob);
        when(blob.getContent()).thenReturn("objectFile".getBytes());

        GcsClient gcsClient = new GcsClient(storage);
        byte[] actualValue = gcsClient.getFile(pythonFile);

        verify(storage, times(1)).get(BlobId.of(bucketName, objectName));
        verify(blob, times(1)).getContent();
        Assert.assertEquals(expectedValue, Arrays.toString(actualValue));
    }
}
