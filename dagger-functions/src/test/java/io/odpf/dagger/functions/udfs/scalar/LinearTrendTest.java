package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LinearTrendTest {

    private LinearTrend linearTrend;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "LinearTrend")).thenReturn(metricGroup);
        linearTrend = new LinearTrend();
    }

    @Test
    public void shouldReturnGradientWhenNoMetricValueIsMissingInHopWindow() {
        ArrayList<Timestamp> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<Double>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        int windowSize = 5;

        timestampsList.add(new Timestamp(timeInMillis));
        timestampsList.add(new Timestamp(timeInMillis + 60000));
        timestampsList.add(new Timestamp(timeInMillis + 120000));
        timestampsList.add(new Timestamp(timeInMillis + 180000));
        timestampsList.add(new Timestamp(timeInMillis + 240000));

        metricList.add(0.0);
        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(4.0);
        metricList.add(4.0);

        double beta = linearTrend.eval(timestampsList, metricList, hopStartTime, windowSize);
        assertEquals(1.0, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenTwoMetricValuesAreMissingInHopWindow() {
        ArrayList<Timestamp> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<Double>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        int windowSize = 5;

        timestampsList.add(new Timestamp(timeInMillis));
        timestampsList.add(new Timestamp(timeInMillis + 120000));
        timestampsList.add(new Timestamp(timeInMillis + 240000));

        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(4.0);

        double beta = linearTrend.eval(timestampsList, metricList, hopStartTime, windowSize);
        assertEquals(0.4, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenThreeMetricValuesAreMissingInHopWindow() {
        ArrayList<Timestamp> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<Double>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        int windowSize = 5;

        timestampsList.add(new Timestamp(timeInMillis + 60000));
        timestampsList.add(new Timestamp(timeInMillis + 180000));

        metricList.add(2.0);
        metricList.add(1.0);

        double beta = linearTrend.eval(timestampsList, metricList, hopStartTime, windowSize);
        assertEquals(-0.1, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenOnlyOneValueInHopWindow() {
        ArrayList<Timestamp> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        int windowSize = 5;

        timestampsList.add(new Timestamp(timeInMillis + 60000));

        metricList.add(2.0);

        double beta = linearTrend.eval(timestampsList, metricList, hopStartTime, windowSize);
        assertEquals(-0.2, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenNoValueInHopWindow() {
        ArrayList<Timestamp> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        int windowSize = 5;

        double beta = linearTrend.eval(timestampsList, metricList, hopStartTime, windowSize);
        assertEquals(0.0, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenAllMetricValuesAreNonEmptyAndSameInHopWindow() {
        ArrayList<Timestamp> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        int windowSize = 5;

        timestampsList.add(new Timestamp(timeInMillis));
        timestampsList.add(new Timestamp(timeInMillis + 60000));
        timestampsList.add(new Timestamp(timeInMillis + 120000));
        timestampsList.add(new Timestamp(timeInMillis + 180000));
        timestampsList.add(new Timestamp(timeInMillis + 240000));

        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(2.0);

        double beta = linearTrend.eval(timestampsList, metricList, hopStartTime, windowSize);
        assertEquals(0.0, beta, 0);
    }

    @Test
    public void shouldReturnCorrectGradientWithOutOfOrderValues() {
        ArrayList<Timestamp> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        int windowSize = 20;

        timestampsList.add(new Timestamp(timeInMillis + 60000));
        timestampsList.add(new Timestamp(timeInMillis + 180000));
        timestampsList.add(new Timestamp(timeInMillis + 300000));
        timestampsList.add(new Timestamp(timeInMillis + 1140000));
        timestampsList.add(new Timestamp(timeInMillis + 420000));
        timestampsList.add(new Timestamp(timeInMillis + 480000));
        timestampsList.add(new Timestamp(timeInMillis + 600000));
        timestampsList.add(new Timestamp(timeInMillis + 660000));

        metricList.add(1.0);
        metricList.add(1.0);
        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(1.0);

        double beta = linearTrend.eval(timestampsList, metricList, hopStartTime, windowSize);
        assertEquals(-0.030827067669172932, beta, 0);
    }

    @Test
    public void shouldReturnCorrectGradientWithDuplicateValues() {
        ArrayList<Timestamp> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();
        Timestamp hopStartTime = new Timestamp(timeInMillis);

        int windowSize = 20;

        timestampsList.add(new Timestamp(timeInMillis + 60000));
        timestampsList.add(new Timestamp(timeInMillis + 180000));
        timestampsList.add(new Timestamp(timeInMillis + 300000));
        timestampsList.add(new Timestamp(timeInMillis + 420000));
        timestampsList.add(new Timestamp(timeInMillis + 480000));
        timestampsList.add(new Timestamp(timeInMillis + 600000));
        timestampsList.add(new Timestamp(timeInMillis + 660000));
        timestampsList.add(new Timestamp(timeInMillis + 660000));
        timestampsList.add(new Timestamp(timeInMillis + 1140000));

        metricList.add(1.0);
        metricList.add(1.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(1.0);
        metricList.add(1.0);
        metricList.add(1.0);

        double beta = linearTrend.eval(timestampsList, metricList, hopStartTime, windowSize);
        assertEquals(-0.030827067669172932, beta, 0);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        linearTrend.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
