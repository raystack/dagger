package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.common.udfs.AggregateUdf;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.DistinctCountAccumulator;

/**
 * User-defined aggregate function to get Distinct count.
 */
public class DistinctCount extends AggregateUdf<Integer, DistinctCountAccumulator> {

    @Override
    public DistinctCountAccumulator createAccumulator() {
        return new DistinctCountAccumulator();
    }

    @Override
    public Integer getValue(DistinctCountAccumulator distinctCountAccumulator) {
        return distinctCountAccumulator.count();
    }

    /**
     * returns distinct count of a field in input stream.
     *
     * @param distinctCountAccumulator the distinct count accumulator
     * @param item                     fieldName
     * @author prakhar.m
     */
    public void accumulate(DistinctCountAccumulator distinctCountAccumulator, String item) {
        if (item == null) {
            return;
        }
        distinctCountAccumulator.add(item);
    }

    public void merge(DistinctCountAccumulator distinctCountAccumulator, Iterable<DistinctCountAccumulator> it) {
        for (DistinctCountAccumulator distinctCountAccumulatorInstance : it) {
            distinctCountAccumulator.getDistinctItems().addAll(distinctCountAccumulatorInstance.getDistinctItems());
        }
    }
}
