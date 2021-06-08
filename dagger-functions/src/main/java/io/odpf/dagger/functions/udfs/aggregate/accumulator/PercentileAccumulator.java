package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PercentileAccumulator implements Serializable {
    private List<Double> dValueList = new ArrayList<>();
    private double percentile;

    public void add(double p, double dValue) {
        percentile = p;
        dValueList.add(dValue);
    }

    public double getPercentileValue() {
        return new Percentile(this.percentile).
                evaluate(dValueList.stream().sorted().mapToDouble(Double::doubleValue).toArray(), 0, dValueList.size());
    }
}

