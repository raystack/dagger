package com.gotocompany.dagger.core.processors.longbow.outputRow;

import org.apache.flink.types.Row;

/**
 * The Output identity.
 */
public class OutputIdentity implements WriterOutputRow {
    @Override
    public Row get(Row input) {
        return input;
    }
}
