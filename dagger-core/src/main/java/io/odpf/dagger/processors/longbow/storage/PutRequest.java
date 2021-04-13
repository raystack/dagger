package io.odpf.dagger.processors.longbow.storage;

import org.apache.hadoop.hbase.client.Put;

public interface PutRequest {
    Put get();

    String getTableId();
}
