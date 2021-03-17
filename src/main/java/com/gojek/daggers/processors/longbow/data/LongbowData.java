package com.gojek.daggers.processors.longbow.data;

import org.apache.hadoop.hbase.client.Result;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface LongbowData extends Serializable {
    Map parse(List<Result> scanResult);
}
