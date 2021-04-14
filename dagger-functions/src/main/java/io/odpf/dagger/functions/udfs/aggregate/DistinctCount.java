package io.odpf.dagger.functions.udfs.aggregate;

import org.apache.flink.table.functions.AggregateFunction;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.distinctcount.DistinctCountAccumulator;

public class DistinctCount extends AggregateFunction<Integer, DistinctCountAccumulator> {

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
}
