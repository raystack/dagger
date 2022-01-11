package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;
import org.apache.flink.table.annotation.DataTypeHint;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The ExponentialMovingAverage udf.
 */
public class ExponentialMovingAverage extends ScalarUdf {
    private static final long MILLI_SECONDS_IN_MINUTE = 60000;

    /**
     * Calculates exponential moving average (at per minute frequency) using a list of non-null values.
     *
     * @param timestampsObjectArray the timestamps array
     * @param valuesArray     the values array
     * @param hopStartTime    the hop start time
     * @param window          the window
     * @param alpha           the alpha
     * @return the double
     */
    public double eval(@DataTypeHint(value = "RAW", bridgedTo = ArrayList.class) ArrayList<Object> timestampsObjectArray, @DataTypeHint(value = "RAW", bridgedTo = ArrayList.class) ArrayList<Object> valuesArray, LocalDateTime hopStartTime, Double window, Double alpha) {
        List<Timestamp> timestampsArray = timestampsObjectArray.stream().map(o -> Timestamp.valueOf((LocalDateTime) o)).collect(Collectors.toList());
        SortedMap<Double, Double> positionSortedValues = sortValuesByTime(Timestamp.valueOf(hopStartTime), window, timestampsArray, valuesArray);

        return calculateEMA(positionSortedValues, window, alpha);
    }

    /**
     * Calculate ema double.
     *
     * @param positionSortedValues the position sorted values
     * @param window               the window
     * @param alpha                the alpha
     * @return the double
     */
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

    /**
     * Sort values by time sorted map.
     *
     * @param hopStartTime    the hop start time
     * @param window          the window
     * @param timestampsArray the timestamps array
     * @param valuesArray     the values array
     * @return the sorted map
     */
    public static SortedMap<Double, Double> sortValuesByTime(Timestamp hopStartTime, Double window, List<Timestamp> timestampsArray, ArrayList<Object> valuesArray) {
        SortedMap<Double, Double> positionSortedValues = new TreeMap<>();
        int i;

        for (i = 0; i < timestampsArray.size(); i++) {
            Timestamp startTime = timestampsArray.get(i);
            double value = (double) valuesArray.get(i);

            double position = getPosition(startTime, hopStartTime, window);

            positionSortedValues.put(position, value);
        }

        return positionSortedValues;
    }

    /**
     * Gets position.
     *
     * @param startTime    the start time
     * @param hopStartTime the hop start time
     * @param window       the window
     * @return the position
     */
    public static double getPosition(Timestamp startTime, Timestamp hopStartTime, Double window) {
        long hopStartMS = hopStartTime.getTime();
        long startMS = startTime.getTime();

        long reversePosition = Math.round((startMS - hopStartMS)) / MILLI_SECONDS_IN_MINUTE;

        double position = window - 1.0 - (double) reversePosition;

        return position;
    }
}
