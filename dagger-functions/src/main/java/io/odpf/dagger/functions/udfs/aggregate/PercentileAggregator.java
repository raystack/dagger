package io.odpf.dagger.functions.udfs.aggregate;


import io.odpf.dagger.common.udfs.AggregateUdf;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.PercentileAccumulator;
import org.apache.flink.table.annotation.DataTypeHint;

import java.math.BigDecimal;


/**
 * User-defined aggregate function to get Percentile.
 *
 * @author lavkesh.lahngir
 * @team lens @go-jek.com
 */
public class PercentileAggregator extends AggregateUdf<Double, PercentileAccumulator> {

    @Override
    public PercentileAccumulator createAccumulator() {
        return new PercentileAccumulator();
    }

    @Override
    public Double getValue(PercentileAccumulator acc) {
        return acc.getPercentileValue();
    }

    /**
     * Accumulate.
     *
     * @param acc        the acc
     * @param percentile the percentile
     * @param dValue     the d value
     */
    public void accumulate(PercentileAccumulator acc, @DataTypeHint("DECIMAL(30, 3)") BigDecimal percentile, @DataTypeHint("DECIMAL(30, 3)") BigDecimal dValue) {
        acc.add(percentile.doubleValue(), dValue.doubleValue());
    }

    public void merge(PercentileAccumulator percentileAccumulator, Iterable<PercentileAccumulator> otherAccumulators) {
        for (PercentileAccumulator accumulatorInstance : otherAccumulators) {
            percentileAccumulator.getdValueList().addAll(accumulatorInstance.getdValueList());
            percentileAccumulator.setPercentile(accumulatorInstance.getPercentile());
        }
    }
}

