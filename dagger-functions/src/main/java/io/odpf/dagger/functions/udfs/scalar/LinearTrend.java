package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;
import org.apache.flink.table.annotation.DataTypeHint;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The Linear trend udf.
 */
public class LinearTrend extends ScalarUdf {
    private static final long MILLI_SECONDS_IN_MINUTE = 60000;

    /**
     * returns the gradient of the best fit line of the list of non-null demand values given the defined time window.
     *
     * @param localDateTimeArray    the timestamps array
     * @param values                the values
     * @param hopStartTime          the hop start time
     * @param windowLengthInMinutes the window length in minutes
     * @return the double
     */
    public double eval(@DataTypeHint(value = "RAW", bridgedTo = ArrayList.class) ArrayList<LocalDateTime> localDateTimeArray, @DataTypeHint(value = "RAW", bridgedTo = ArrayList.class) ArrayList<Double> values, LocalDateTime hopStartTime, Integer windowLengthInMinutes) {
        ArrayList<Timestamp> timestamps = new ArrayList<Timestamp>();
        localDateTimeArray.forEach(localDateTime -> timestamps.add(Timestamp.valueOf(localDateTime)));
        return calculateLinearTrend(timestamps, values, Timestamp.valueOf(hopStartTime), windowLengthInMinutes);
    }

    private double calculateLinearTrend(ArrayList<Timestamp> timestampsArray, ArrayList<Double> valueList, Timestamp hopStartTime, Integer windowLengthInMinutes) {
        ArrayList<Double> hopWindowList = IntStream.range(0, windowLengthInMinutes).mapToObj(i -> (double) i).collect(Collectors.toCollection(ArrayList::new));
        ArrayList<Double> orderedValueList = getOrderedValueList(hopStartTime, valueList, timestampsArray, windowLengthInMinutes);

        double timeValueCovariance = getCovariance(hopWindowList, orderedValueList, windowLengthInMinutes);
        double timeVariance = getVariance(hopWindowList, windowLengthInMinutes);
        return (timeValueCovariance / timeVariance);
    }

    private ArrayList<Double> getOrderedValueList(Timestamp hopStartTime, ArrayList<Double> valueList, ArrayList<Timestamp> timestampsArray, int windowLengthInMinutes) {
        ArrayList<Double> orderedValueList = new ArrayList<>(Collections.nCopies(windowLengthInMinutes, 0d));
        IntStream.range(0, valueList.size()).forEach(index -> {
            double value = valueList.get(index);
            Timestamp valueStartTime = timestampsArray.get(index);
            int position = getPosition(valueStartTime, hopStartTime);
            orderedValueList.set(position, value);
        });
        return orderedValueList;
    }

    private int getPosition(Timestamp valueStartTime, Timestamp hopStartTime) {
        long hopStartMS = hopStartTime.getTime();
        long valueStartMS = valueStartTime.getTime();

        long deltaInMinute = ((valueStartMS - hopStartMS) / MILLI_SECONDS_IN_MINUTE);
        return (int) deltaInMinute;
    }

    private double getVariance(ArrayList<Double> list, int hopWindowLength) {
        return getSumOfAnArray(getSquareArray(list)) - Math.pow(getSumOfAnArray(list), 2) / hopWindowLength;
    }

    private double getCovariance(ArrayList<Double> listOne, ArrayList<Double> listTwo, int hopWindowLength) {
        return getSumOfAnArray(multiplyListsOfSameLength(listOne, listTwo)) - getSumOfAnArray(listOne) * getSumOfAnArray(listTwo) / hopWindowLength;
    }

    private ArrayList<Double> multiplyListsOfSameLength(ArrayList<Double> listOne, ArrayList<Double> listTwo) {
        ArrayList<Double> arrayAfterMultiplication = new ArrayList<>();
        IntStream.range(0, listOne.size()).forEach(index -> arrayAfterMultiplication.add(index, listOne.get(index) * listTwo.get(index)));
        return arrayAfterMultiplication;
    }

    private ArrayList<Double> getSquareArray(ArrayList<Double> array) {
        return array.stream().map(element -> element * element).collect(Collectors.toCollection(ArrayList::new));
    }

    private double getSumOfAnArray(ArrayList<Double> array) {
        return array.stream().mapToDouble(element -> element).sum();
    }
}
