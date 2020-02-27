package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.postProcessors.longbow.validator.LongbowType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.stream.IntStream;

public class AppendMetaData implements MapFunction<Row, Row> {
    private static final int LONGBOW_EXTRA_FIELDS = 3;
    private String inputProtoClassName;
    private LongbowSchema longbowSchema;
    private String tableId;


    public AppendMetaData(String inputProtoClassName, LongbowSchema longbowSchema, String tableId) {
        this.inputProtoClassName = inputProtoClassName;
        this.longbowSchema = longbowSchema;
        this.tableId = tableId;
    }

    @Override
    public Row map(Row row) {
        int inputRowSize = row.getArity();
        Row outputRow = new Row(inputRowSize + LONGBOW_EXTRA_FIELDS);
        IntStream.range(0, inputRowSize).forEach(i -> outputRow.setField(i, row.getField(i)));
        outputRow.setField(inputRowSize, tableId);
        outputRow.setField(inputRowSize + 1, inputProtoClassName);
        outputRow.setField(inputRowSize + 2, longbowSchema.getValue(row, LongbowType.LongbowWrite.getKeyName()));
        return outputRow;
    }
}
