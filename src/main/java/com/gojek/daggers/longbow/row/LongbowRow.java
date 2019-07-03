package com.gojek.daggers.longbow.row;

import org.apache.flink.types.Row;

import java.io.Serializable;

public interface LongbowRow extends Serializable {
    byte[] getLatest(Row input);

    byte[] getEarliest(Row input);

    String[] getInvalidFields();
}
