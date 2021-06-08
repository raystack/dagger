package io.odpf.dagger.functions.udfs.table.outlier.mad;

import java.sql.Timestamp;

public class Point {
    private final Timestamp timestamp;
    private final Double value;
    private final boolean observable;
    private Double distanceFromMad;
    private boolean isOutlier;
    private double upperBound;
    private double lowerBound;

    public static final Point EMPTY_POINT = new Point(null, 0d, false);

    public Point(Timestamp timestamp, Double value, boolean observable) {
        this.timestamp = timestamp;
        this.value = value;
        this.upperBound = value;
        this.lowerBound = value;
        this.observable = observable;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Double getValue() {
        return value;
    }

    public boolean isObservable() {
        return observable;
    }

    public Double getDistanceFromMad() {
        return distanceFromMad;
    }

    public boolean isOutlier() {
        return isOutlier;
    }

    public double getUpperBound() {
        return upperBound;
    }

    public double getLowerBound() {
        return lowerBound;
    }

    public void setOutlier(boolean outlier) {
        isOutlier = outlier;
    }

    public void setUpperBound(double upperBound) {
        this.upperBound = upperBound;
    }

    public void setLowerBound(double lowerBound) {
        this.lowerBound = lowerBound;
    }

    public void setDistanceFromMad(Double distanceFromMad) {
        this.distanceFromMad = distanceFromMad;
    }
}
