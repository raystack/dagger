package com.gojek.daggers.postProcessors.longbow.outputRow;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.validator.LongbowType;
import org.apache.flink.types.Row;

import java.util.stream.IntStream;

public class OutputSynchronizer implements WriterOutputRow {
    private LongbowSchema longbowSchema;
    private String tableId;
    private String inputProto;

    public OutputSynchronizer(LongbowSchema longbowSchema, String tableId, String inputProto) {
        this.longbowSchema = longbowSchema;
        this.tableId = tableId;
        this.inputProto = inputProto;
    }

    @Override
    public Row get(Row input) {
        int outputArity = input.getArity() + 3;
        int inputArity = input.getArity();
        Row output = new Row(outputArity);
        IntStream.range(0, inputArity).forEach(i -> output.setField(i, input.getField(i)));
        output.setField(inputArity, tableId);
        output.setField(inputArity + 1, inputProto);
        output.setField(inputArity + 2, longbowSchema.getValue(input, LongbowType.LongbowWrite.getKeyName()));
        return output;
    }
}
