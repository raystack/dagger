package com.gojek.daggers.postProcessors.longbow.storage;

import org.apache.hadoop.hbase.client.Put;

public interface PutRequest {
    Put get();
}
