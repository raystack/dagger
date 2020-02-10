package com.gojek.daggers.postProcessors.longbow.processor;

import org.apache.hadoop.hbase.client.Result;

import java.util.List;
import java.util.Map;

public interface LongbowData {
    public Map parse(List<Result> scanResult);
}
