package com.gotocompany.dagger.functions.udfs.scalar.dart.store.gcs;


import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.gotocompany.dagger.common.metrics.managers.GaugeStatsManager;
import com.gotocompany.dagger.functions.exceptions.BucketDoesNotExistException;
import com.gotocompany.dagger.functions.exceptions.TagDoesNotExistException;
import com.gotocompany.dagger.functions.udfs.scalar.dart.DartAspects;

import static com.gotocompany.dagger.common.core.Constants.UDF_TELEMETRY_GROUP_KEY;

/**
 * The type Gcs client.
 */
public class GcsClient {

    private Storage storage;

    private static final Double BYTES_TO_KB = 1024.0;
    private static final String DART_PATH = "dartpath";

    /**
     * Instantiates a new Gcs client.
     *
     * @param projectId the project id
     */
    public GcsClient(String projectId) {

        if (storage == null) {
            storage = StorageOptions.newBuilder()
                    .setProjectId(projectId)
                    .build().getService();

        }
    }

    /**
     * Fetch json data string.
     *
     * @param udfName           the udf name
     * @param gaugeStatsManager the gauge stats manager
     * @param bucketName        the bucket name
     * @param dartName          the dart name
     * @return the string
     * @throws TagDoesNotExistException    the tag does not exist exception
     * @throws BucketDoesNotExistException the bucket does not exist exception
     */
    public String fetchJsonData(String udfName, GaugeStatsManager gaugeStatsManager, String bucketName, String dartName) throws TagDoesNotExistException, BucketDoesNotExistException {

        Bucket bucket = storage.get(bucketName);
        if (bucket == null) {
            throw new BucketDoesNotExistException(String.format("Could not find the bucket in gcs for %s", dartName));
        }

        Blob blob = storage.get(BlobId.of(bucketName, dartName));

        if (blob == null) {
            throw new TagDoesNotExistException(String.format("Could not find the content in gcs for %s", dartName));
        }
        gaugeStatsManager.registerString(UDF_TELEMETRY_GROUP_KEY, udfName, DartAspects.DART_GCS_PATH.getValue(), dartName);
        gaugeStatsManager.registerDouble(DART_PATH, dartName, DartAspects.DART_GCS_FILE_SIZE.getValue(), blob.getContent().length / BYTES_TO_KB);

        return new String(blob.getContent());
    }
}
