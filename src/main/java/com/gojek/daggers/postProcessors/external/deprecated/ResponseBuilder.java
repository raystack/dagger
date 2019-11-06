package com.gojek.daggers.postProcessors.external.deprecated;

import org.apache.flink.types.Row;

import java.io.Serializable;

public class ResponseBuilder implements Serializable {
    private Row parentRow;

    public ResponseBuilder(int size) {
        parentRow = new Row(size);
    }

    public ResponseBuilder(Row row) {
        parentRow = row;
    }


    public ResponseBuilder with(int index, Object value) {
        parentRow.setField(index, value);
        return this;
    }

    public Row build() {
        return parentRow;
    }

}
