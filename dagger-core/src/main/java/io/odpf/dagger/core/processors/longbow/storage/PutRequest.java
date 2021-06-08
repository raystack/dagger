package io.odpf.dagger.core.processors.longbow.storage;

import org.apache.hadoop.hbase.client.Put;

/**
 * The interface Put request.
 */
public interface PutRequest {
    /**
     * Get put.
     *
     * @return the put
     */
    Put get();

    /**
     * Gets table id.
     *
     * @return the table id
     */
    String getTableId();
}
