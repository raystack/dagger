package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The accumulator for Percentile udf.
 */
public class PercentileAccumulator implements Serializable {

    private List<Double> dValueList = new ArrayList<>();

    private double percentile;

    /**
     * Add percentile.
     *
     * @param percentileValue   the percentileValue
     * @param dValue            the double value
     */
    public void add(double percentileValue, double dValue) {
        percentile = percentileValue;
        dValueList.add(dValue);
    }

    /**
     * Gets percentile value.
     *
     * @return the percentile value
     */
    public double getPercentileValue() {
        return new Percentile(this.percentile).
                evaluate(dValueList.stream().sorted().mapToDouble(Double::doubleValue).toArray(), 0, dValueList.size());
    }

    public List<Double> getdValueList() {
        return dValueList;
    }

    public void setdValueList(List<Double> dValueList) {
        this.dValueList = dValueList;
    }

    public double getPercentile() {
        return percentile;
    }

    public void setPercentile(double percentile) {
        this.percentile = percentile;
    }
}

