package org.raystack.dagger.core.processors.longbow.outputRow;

import org.raystack.dagger.core.processors.longbow.validator.LongbowType;
import org.raystack.dagger.core.utils.Constants;
import org.raystack.dagger.core.processors.longbow.LongbowSchema;

import org.apache.flink.types.Row;

import java.util.stream.IntStream;

/**
 * The Output synchronizer.
 */
public class OutputSynchronizer implements WriterOutputRow {
    private LongbowSchema longbowSchema;
    private String tableId;
    private String inputProto;

    /**
     * Instantiates a new Output synchronizer.
     *
     * @param longbowSchema the longbow schema
     * @param tableId       the table id
     * @param inputProto    the input proto
     */
    public OutputSynchronizer(LongbowSchema longbowSchema, String tableId, String inputProto) {
        this.longbowSchema = longbowSchema;
        this.tableId = tableId;
        this.inputProto = inputProto;
    }

    @Override
    public Row get(Row input) {
        int outputArity = input.getArity() + Constants.LONGBOW_OUTPUT_ADDITIONAL_ARITY;
        int inputArity = input.getArity();
        Row output = new Row(outputArity);
        IntStream.range(0, inputArity).forEach(i -> output.setField(i, input.getField(i)));
        output.setField(inputArity, tableId);
        output.setField(inputArity + 1, inputProto);
        output.setField(inputArity + 2, longbowSchema.getValue(input, LongbowType.LongbowWrite.getKeyName()));
        return output;
    }
}
