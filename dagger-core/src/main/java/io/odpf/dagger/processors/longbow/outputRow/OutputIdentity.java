package io.odpf.dagger.processors.longbow.outputRow;

import org.apache.flink.types.Row;

public class OutputIdentity implements WriterOutputRow {
    @Override
    public Row get(Row input) {
        return input;
    }
}
