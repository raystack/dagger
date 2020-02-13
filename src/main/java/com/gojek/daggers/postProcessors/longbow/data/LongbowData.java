package com.gojek.daggers.postProcessors.longbow.data;

import org.apache.hadoop.hbase.client.Result;

import java.util.List;
import java.util.Map;

public interface LongbowData {
    Map parse(List<Result> scanResult);
}
