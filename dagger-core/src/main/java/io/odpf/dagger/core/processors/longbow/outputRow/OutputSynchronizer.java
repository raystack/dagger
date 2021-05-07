package io.odpf.dagger.core.processors.longbow.outputRow;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.validator.LongbowType;

import org.apache.flink.types.Row;

import java.util.stream.IntStream;

import static io.odpf.dagger.core.utils.Constants.ADDITIONAL_ARITY;

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
        int outputArity = input.getArity() + ADDITIONAL_ARITY;
        int inputArity = input.getArity();
        Row output = new Row(outputArity);
        IntStream.range(0, inputArity).forEach(i -> output.setField(i, input.getField(i)));
        output.setField(inputArity, tableId);
        output.setField(inputArity + 1, inputProto);
        output.setField(inputArity + 2, longbowSchema.getValue(input, LongbowType.LongbowWrite.getKeyName()));
        return output;
    }
}
