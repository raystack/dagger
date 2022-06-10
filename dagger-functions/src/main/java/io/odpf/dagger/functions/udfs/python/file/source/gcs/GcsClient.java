package io.odpf.dagger.functions.udfs.python.file.source.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The type Gcs client.
 */
public class GcsClient {

    private Storage storage;

    /**
     * Instantiates a new Gcs client.
     */
    public GcsClient() {

        if (storage == null) {
            storage = StorageOptions.newBuilder()
                    .build().getService();
        }
    }

    /**
     * Instantiates a new Gcs client.
     * This constructor used for unit test purposes.
     *
     * @param storage the storage
     */
    public GcsClient(Storage storage) {
        this.storage = storage;
    }

    /**
     * Get file byte [ ].
     *
     * @param pythonFile the python file
     * @return the byte [ ]
     */
    public byte[] getFile(String pythonFile) {
        List<String> file = Arrays.asList(pythonFile.replace("gs://", "").split("/"));

        String bucketName = file.get(0);
        String objectName = file.stream().skip(1).collect(Collectors.joining("/"));

        Blob blob = storage.get(BlobId.of(bucketName, objectName));

        return blob.getContent();
    }
}
