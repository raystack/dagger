package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;

import static org.junit.Assert.*;
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
        ArrayList<LocalDateTime> localDateTimes = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<Double>();

        long timeInMillis = System.currentTimeMillis();

        int windowSize = 5;
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault());
        localDateTimes.add(localDateTime);
        localDateTimes.add(localDateTime.plusSeconds(60));
        localDateTimes.add(localDateTime.plusSeconds(120));
        localDateTimes.add(localDateTime.plusSeconds(180));
        localDateTimes.add(localDateTime.plusSeconds(240));

        metricList.add(0.0);
        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(4.0);
        metricList.add(4.0);

        double beta = linearTrend.eval(localDateTimes, metricList, localDateTime, windowSize);
        assertEquals(1.0, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenTwoMetricValuesAreMissingInHopWindow() {
        ArrayList<LocalDateTime> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<Double>();

        long timeInMillis = System.currentTimeMillis();

        int windowSize = 5;
        LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault());
        timestampsList.add(now);
        timestampsList.add(now.plusSeconds(120));
        timestampsList.add(now.plusSeconds(240));

        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(4.0);
        double beta = linearTrend.eval(timestampsList, metricList, now, windowSize);
        assertEquals(0.4, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenThreeMetricValuesAreMissingInHopWindow() {
        ArrayList<LocalDateTime> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<Double>();

        long timeInMillis = System.currentTimeMillis();

        int windowSize = 5;
        LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault());
        timestampsList.add(now.plusSeconds(60));
        timestampsList.add(now.plusSeconds(180));

        metricList.add(2.0);
        metricList.add(1.0);

        double beta = linearTrend.eval(timestampsList, metricList, now, windowSize);
        assertEquals(-0.1, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenOnlyOneValueInHopWindow() {
        ArrayList<LocalDateTime> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();
        LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault());
        int windowSize = 5;

        timestampsList.add(now.plusSeconds(60));

        metricList.add(2.0);

        double beta = linearTrend.eval(timestampsList, metricList, now, windowSize);
        assertEquals(-0.2, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenNoValueInHopWindow() {
        ArrayList<LocalDateTime> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();


        int windowSize = 5;

        double beta = linearTrend.eval(timestampsList, metricList, LocalDateTime.now(), windowSize);
        assertEquals(0.0, beta, 0);
    }

    @Test
    public void shouldReturnGradientWhenAllMetricValuesAreNonEmptyAndSameInHopWindow() {
        ArrayList<LocalDateTime> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();
        LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault());
        int windowSize = 5;

        timestampsList.add(now);
        timestampsList.add(now.plusSeconds(60));
        timestampsList.add(now.plusSeconds(120));
        timestampsList.add(now.plusSeconds(180));
        timestampsList.add(now.plusSeconds(240));

        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(2.0);

        double beta = linearTrend.eval(timestampsList, metricList, now, windowSize);
        assertEquals(0.0, beta, 0);
    }

    @Test
    public void shouldReturnCorrectGradientWithOutOfOrderValues() {
        ArrayList<LocalDateTime> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();

        int windowSize = 20;
        LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault());
        timestampsList.add(now.plusSeconds(60));
        timestampsList.add(now.plusSeconds(180));
        timestampsList.add(now.plusSeconds(300));
        timestampsList.add(now.plusSeconds(1140));
        timestampsList.add(now.plusSeconds(420));
        timestampsList.add(now.plusSeconds(480));
        timestampsList.add(now.plusSeconds(600));
        timestampsList.add(now.plusSeconds(660));

        metricList.add(1.0);
        metricList.add(1.0);
        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(1.0);

        double beta = linearTrend.eval(timestampsList, metricList, now, windowSize);
        assertEquals(-0.030827067669172932, beta, 0);
    }

    @Test
    public void shouldReturnCorrectGradientWithDuplicateValues() {
        ArrayList<LocalDateTime> timestampsList = new ArrayList<>();
        ArrayList<Double> metricList = new ArrayList<>();

        long timeInMillis = System.currentTimeMillis();
        LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault());
        int windowSize = 20;

        timestampsList.add(now.plusSeconds(60));
        timestampsList.add(now.plusSeconds(180));
        timestampsList.add(now.plusSeconds(300));
        timestampsList.add(now.plusSeconds(420));
        timestampsList.add(now.plusSeconds(480));
        timestampsList.add(now.plusSeconds(600));
        timestampsList.add(now.plusSeconds(660));
        timestampsList.add(now.plusSeconds(660));
        timestampsList.add(now.plusSeconds(1140));

        metricList.add(1.0);
        metricList.add(1.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(2.0);
        metricList.add(1.0);
        metricList.add(1.0);
        metricList.add(1.0);
        metricList.add(1.0);

        double beta = linearTrend.eval(timestampsList, metricList, now, windowSize);
        assertEquals(-0.030827067669172932, beta, 0);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        linearTrend.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
