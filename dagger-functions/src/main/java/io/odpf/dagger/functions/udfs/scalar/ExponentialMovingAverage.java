package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class ExponentialMovingAverage extends ScalarUdf {
    private static final long MILLI_SECONDS_IN_MINUTE = 60000;

    public double eval(ArrayList<Object> timestampsArray, ArrayList<Object> valuesArray, Timestamp hopStartTime, Double window, Double alpha) {
        SortedMap<Double, Double> positionSortedValues = sortValuesByTime(hopStartTime, window, timestampsArray, valuesArray);

        return calculateEMA(positionSortedValues, window, alpha);
    }

    public static double calculateEMA(SortedMap<Double, Double> positionSortedValues, Double window, Double alpha) {
        double emaSum = 0;
        for (Map.Entry<Double, Double> lagValue : positionSortedValues.entrySet()) {
            Double position = lagValue.getKey();
            Double value = lagValue.getValue();

            if (position == window - 1) {
                emaSum += value * Math.pow(1 - alpha, position);
            } else {
                emaSum += value * alpha * Math.pow(1 - alpha, position);
            }
        }

        return emaSum;
    }

    public static SortedMap<Double, Double> sortValuesByTime(Timestamp hopStartTime, Double window, ArrayList<Object> timestampsArray, ArrayList<Object> valuesArray) {
        SortedMap<Double, Double> positionSortedValues = new TreeMap<>();
        int i;

        for (i = 0; i < timestampsArray.size(); i++) {
            Timestamp startTime = (Timestamp) timestampsArray.get(i);
            double value = (double) valuesArray.get(i);

            double position = getPosition(startTime, hopStartTime, window);

            positionSortedValues.put(position, value);
        }

        return positionSortedValues;
    }

    public static double getPosition(Timestamp startTime, Timestamp hopStartTime, Double window) {
        long hopStartMS = hopStartTime.getTime();
        long startMS = startTime.getTime();

        long reversePosition = Math.round((startMS - hopStartMS)) / MILLI_SECONDS_IN_MINUTE;

        double position = window - 1.0 - (double) reversePosition;

        return position;
    }
}
