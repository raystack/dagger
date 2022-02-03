package io.odpf.dagger.functions.udfs.scalar;

import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class ExponentialMovingAverageTest {

    @Test
    public void shouldReturnCaulculation() {
        SortedMap<Double, Double> positionSortedValues = new TreeMap<>();
        double window = 6;
        double alpha = 0.2;
        //long timeInMillis = System.currentTimeMillis();

        positionSortedValues.put(0.0, 7.0);
        positionSortedValues.put(3.0, 3.0);
        positionSortedValues.put(4.0, 4.0);
        positionSortedValues.put(5.0, 1.0);

        assertEquals(2.36256, ExponentialMovingAverage.calculateEMA(positionSortedValues, window, alpha), 0.0000001);
    }

    @Test
    public void shouldReturnCorrectSortedValues() {
        ArrayList<Timestamp> timestampsArray = new ArrayList<>();
        ArrayList<Object> valuesArray = new ArrayList<Object>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        double window = 6.0;

        timestampsArray.add(new Timestamp(timeInMillis));
        timestampsArray.add(new Timestamp(timeInMillis + 60000));
        timestampsArray.add(new Timestamp(timeInMillis + 120000));
        timestampsArray.add(new Timestamp(timeInMillis + 300000));

        valuesArray.add(1.0);
        valuesArray.add(4.0);
        valuesArray.add(3.0);
        valuesArray.add(7.0);

        SortedMap<Double, Double> expected = new TreeMap<>();
        expected.put(0.0, 7.0);
        expected.put(3.0, 3.0);
        expected.put(4.0, 4.0);
        expected.put(5.0, 1.0);

        assertEquals(expected, ExponentialMovingAverage.sortValuesByTime(hopStartTime, window, timestampsArray, valuesArray));
    }

    @Test
    public void shouldReturnCorrectPosition() {
        long timeInMillis = System.currentTimeMillis();
        Timestamp startTime = new Timestamp(timeInMillis + 180000);
        Timestamp hopStartTime = new Timestamp(timeInMillis);
        double window = 6.0;

        assertEquals(2.0, ExponentialMovingAverage.getPosition(startTime, hopStartTime, window), 0.0000001);
    }
}
