package com.gojek.daggers.postprocessors.longbow.outputRow;

import org.apache.flink.types.Row;

import java.io.Serializable;

public interface WriterOutputRow extends Serializable {
    Row get(Row input);
}
