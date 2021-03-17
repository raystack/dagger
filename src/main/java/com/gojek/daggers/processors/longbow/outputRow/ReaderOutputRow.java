package com.gojek.daggers.processors.longbow.outputRow;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Map;

public interface ReaderOutputRow extends Serializable {
    Row get(Map<String, Object> scanResult, Row input);
}
