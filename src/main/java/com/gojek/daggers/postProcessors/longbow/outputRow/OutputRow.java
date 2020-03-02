package com.gojek.daggers.postProcessors.longbow.outputRow;

import org.apache.flink.types.Row;

import java.util.Map;

public interface OutputRow {
    Row get(Map<String, Object> scanResult, Row input);
}
