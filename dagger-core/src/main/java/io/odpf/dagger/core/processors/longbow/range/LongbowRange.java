package io.odpf.dagger.core.processors.longbow.range;

import org.apache.flink.types.Row;

import java.io.Serializable;

public interface LongbowRange extends Serializable {
    byte[] getUpperBound(Row input);

    byte[] getLowerBound(Row input);

    String[] getInvalidFields();
}
