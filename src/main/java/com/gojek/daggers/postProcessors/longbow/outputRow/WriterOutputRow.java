package com.gojek.daggers.postProcessors.longbow.outputRow;

import org.apache.flink.types.Row;

public interface WriterOutputRow {
    Row get(Row input);
}
