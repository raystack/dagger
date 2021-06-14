package io.odpf.dagger.functions.udfs.table.outlier.mad;

import java.sql.Timestamp;

/**
 * The Point for OutlierMad udf.
 */
public class Point {
    private final Timestamp timestamp;
    private final Double value;
    private final boolean observable;
    private Double distanceFromMad;
    private boolean isOutlier;
    private double upperBound;
    private double lowerBound;

    public static final Point EMPTY_POINT = new Point(null, 0d, false);

    /**
     * Instantiates a new Point.
     *
     * @param timestamp  the timestamp
     * @param value      the value
     * @param observable the observable
     */
    public Point(Timestamp timestamp, Double value, boolean observable) {
        this.timestamp = timestamp;
        this.value = value;
        this.upperBound = value;
        this.lowerBound = value;
        this.observable = observable;
    }

    /**
     * Gets timestamp.
     *
     * @return the timestamp
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * Gets value.
     *
     * @return the value
     */
    public Double getValue() {
        return value;
    }

    /**
     * Check if it is observable.
     *
     * @return the boolean
     */
    public boolean isObservable() {
        return observable;
    }

    /**
     * Gets distance from mad.
     *
     * @return the distance from mad
     */
    public Double getDistanceFromMad() {
        return distanceFromMad;
    }

    /**
     * Check if it is outlier.
     *
     * @return the boolean
     */
    public boolean isOutlier() {
        return isOutlier;
    }

    /**
     * Gets upper bound.
     *
     * @return the upper bound
     */
    public double getUpperBound() {
        return upperBound;
    }

    /**
     * Gets lower bound.
     *
     * @return the lower bound
     */
    public double getLowerBound() {
        return lowerBound;
    }

    /**
     * Sets outlier.
     *
     * @param outlier the outlier
     */
    public void setOutlier(boolean outlier) {
        isOutlier = outlier;
    }

    /**
     * Sets upper bound.
     *
     * @param upperBound the upper bound
     */
    public void setUpperBound(double upperBound) {
        this.upperBound = upperBound;
    }

    /**
     * Sets lower bound.
     *
     * @param lowerBound the lower bound
     */
    public void setLowerBound(double lowerBound) {
        this.lowerBound = lowerBound;
    }

    /**
     * Sets distance from mad.
     *
     * @param distanceFromMad the distance from mad
     */
    public void setDistanceFromMad(Double distanceFromMad) {
        this.distanceFromMad = distanceFromMad;
    }
}
