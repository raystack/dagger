package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.common.udfs.AggregateUdf;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.ArrayAccumulator;

import java.util.ArrayList;

/**
 * User-defined aggregate function to get the arraList of the objects.
 */
public class CollectArray extends AggregateUdf<ArrayList<Object>, ArrayAccumulator> {

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
    public void accumulate(ArrayAccumulator arrayAccumulator, Object obj) {
        arrayAccumulator.add(obj);
    }

    public ArrayList<Object> getValue(ArrayAccumulator arrayAccumulator) {
        return arrayAccumulator.emit();
    }
}
