package com.gojek.daggers.postprocessors.longbow.storage;

import org.apache.hadoop.hbase.client.Put;

public interface PutRequest {
    Put get();

    String getTableId();
}
