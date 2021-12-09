package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.common.udfs.AggregateUdf;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.ArrayAccumulator;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * User-defined aggregate function to get the arraList of the objects.
 */
@FunctionHint(output = @DataTypeHint(value = "RAW", bridgedTo = ArrayList.class))
public class CollectArray extends AggregateUdf<List<Object>, ArrayAccumulator> {

    public ArrayAccumulator createAccumulator() {
        return new ArrayAccumulator();
    }

    /**
     * Return an arrayList of the objects passed.
     *
     * @param arrayAccumulator the array accumulator
     * @param obj              the obj
     * @return arrayOfObjects arrayList of all the passes objects
     * @author Rasyid
     * @team DE
     */
    public void accumulate(ArrayAccumulator arrayAccumulator, @DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
        arrayAccumulator.add(obj);
    }

    public List<Object> getValue(ArrayAccumulator arrayAccumulator) {
        return arrayAccumulator.emit();
    }

    public void merge(ArrayAccumulator arrayAccumulator, Iterable<ArrayAccumulator> it) {
        for (ArrayAccumulator accumulatorInstance : it) {
            arrayAccumulator.getArrayList().addAll(accumulatorInstance.getArrayList());
        }
    }
}
