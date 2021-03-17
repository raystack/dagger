package com.gojek.daggers.processors.longbow.outputRow;

import org.apache.flink.types.Row;

import java.io.Serializable;

public interface WriterOutputRow extends Serializable {
    Row get(Row input);
}
