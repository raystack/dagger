package com.gojek.daggers.postProcessors.longbow.outputRow;

import org.apache.flink.types.Row;

import java.util.Map;

public class OutputIdentity implements OutputRow {
    @Override
    public Row get(Map<String, Object> scanResult, Row input) {
        return input;
    }
}
