package io.odpf.dagger.functions.udfs.table;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class OutlierMadTest {

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Mock
    private Collector<Tuple5<Timestamp, Double, Double, Double, Boolean>> collector;

    @Captor
    private ArgumentCaptor<Tuple5<Timestamp, Double, Double, Double, Boolean>> tuple5ArgumentCaptor;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "OutlierMad")).thenReturn(metricGroup);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        OutlierMad outlierMad = new OutlierMad();
        outlierMad.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldCollectOutliers() {
        OutlierMad outlierMad = new OutlierMad();
        ArrayList<Double> values = new ArrayList<>();
        values.add(1D);
        values.add(4D);
        values.add(4D);
        values.add(4D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(7D);
        values.add(7D);
        values.add(8D);
        values.add(10D);
        values.add(16D);
        values.add(30D);
        ArrayList<LocalDateTime> timestampArrayList = new ArrayList<>();
        timestampArrayList.add(getLocalDateTimeFromEpoch(60000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(120000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(180000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(240000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(300000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(360000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(420000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(480000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(540000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(600000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(660000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(720000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(780000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(840000L));
        outlierMad.setCollector(collector);
        outlierMad.eval(values, timestampArrayList, getLocalDateTimeFromEpoch(0L), 15, 3, 3, 5);

        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple1 = new Tuple5<>(new Timestamp(720000L), 10D, 11D, -1D, true);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple2 = new Tuple5<>(new Timestamp(780000L), 16D, 11D, -1D, true);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple3 = new Tuple5<>(new Timestamp(840000L), 30D, 11D, -1D, true);
        Mockito.verify(collector, times(3)).collect(tuple5ArgumentCaptor.capture());

        List<Tuple5<Timestamp, Double, Double, Double, Boolean>> allTuples = tuple5ArgumentCaptor.getAllValues();

        Assert.assertEquals(expectedTuple1, allTuples.get(0));
        Assert.assertEquals(expectedTuple2, allTuples.get(1));
        Assert.assertEquals(expectedTuple3, allTuples.get(2));
    }

    @Test
    public void shouldCollectIfNoOutliers() {
        OutlierMad outlierMad = new OutlierMad();
        ArrayList<Double> values = new ArrayList<>();
        values.add(1D);
        values.add(4D);
        values.add(4D);
        values.add(4D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(7D);
        values.add(7D);
        values.add(8D);
        values.add(10D);
        values.add(11D);
        values.add(11D);
        ArrayList<LocalDateTime> timestampArrayList = new ArrayList<>();
        timestampArrayList.add(getLocalDateTimeFromEpoch(60000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(120000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(180000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(240000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(300000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(360000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(420000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(480000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(540000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(600000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(660000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(720000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(780000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(840000L));
        outlierMad.setCollector(collector);
        outlierMad.eval(values, timestampArrayList, getLocalDateTimeFromEpoch(0L), 15, 3, 3, 5);

        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple1 = new Tuple5<>(new Timestamp(720000L), 10D, 11D, -1D, false);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple2 = new Tuple5<>(new Timestamp(780000L), 11D, 11D, -1D, false);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple3 = new Tuple5<>(new Timestamp(840000L), 11D, 11D, -1D, false);
        Mockito.verify(collector, times(3)).collect(tuple5ArgumentCaptor.capture());

        List<Tuple5<Timestamp, Double, Double, Double, Boolean>> allTuples = tuple5ArgumentCaptor.getAllValues();

        Assert.assertEquals(expectedTuple1, allTuples.get(0));
        Assert.assertEquals(expectedTuple2, allTuples.get(1));
        Assert.assertEquals(expectedTuple3, allTuples.get(2));
    }

    @Test
    public void shouldNotSetOutliersIfTotalOutliersLessThanGivenPercentage() {
        OutlierMad outlierMad = new OutlierMad();
        ArrayList<Double> values = new ArrayList<>();
        values.add(1D);
        values.add(4D);
        values.add(4D);
        values.add(4D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(7D);
        values.add(7D);
        values.add(8D);
        values.add(14D);
        values.add(16D);
        values.add(30D);
        ArrayList<LocalDateTime> timestampArrayList = new ArrayList<>();
        timestampArrayList.add(getLocalDateTimeFromEpoch(60000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(120000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(180000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(240000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(300000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(360000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(420000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(480000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(540000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(600000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(660000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(720000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(780000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(840000L));
        outlierMad.setCollector(collector);
        outlierMad.eval(values, timestampArrayList, getLocalDateTimeFromEpoch(0L), 15, 4, 3, 76);

        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple1 = new Tuple5<>(new Timestamp(660000L), 8D, 11D, -1D, false);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple2 = new Tuple5<>(new Timestamp(720000L), 14D, 11D, -1D, false);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple3 = new Tuple5<>(new Timestamp(780000L), 16D, 11D, -1D, false);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple4 = new Tuple5<>(new Timestamp(840000L), 30D, 11D, -1D, false);
        Mockito.verify(collector, times(4)).collect(tuple5ArgumentCaptor.capture());

        List<Tuple5<Timestamp, Double, Double, Double, Boolean>> allTuples = tuple5ArgumentCaptor.getAllValues();

        Assert.assertEquals(expectedTuple1, allTuples.get(0));
        Assert.assertEquals(expectedTuple2, allTuples.get(1));
        Assert.assertEquals(expectedTuple3, allTuples.get(2));
        Assert.assertEquals(expectedTuple4, allTuples.get(3));
    }

    @Test
    public void shouldCollectPointsWithOutlierFalseAndUpperLowerBoundSameAsValueIfMADZero() {
        OutlierMad outlierMad = new OutlierMad();
        ArrayList<Double> values = new ArrayList<>();
        values.add(1D);
        values.add(4D);
        values.add(4D);
        values.add(4D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        values.add(5D);
        ArrayList<LocalDateTime> timestampArrayList = new ArrayList<>();
        timestampArrayList.add(getLocalDateTimeFromEpoch(60000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(120000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(180000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(240000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(300000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(360000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(420000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(480000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(540000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(600000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(660000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(720000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(780000L));
        timestampArrayList.add(getLocalDateTimeFromEpoch(840000L));
        outlierMad.setCollector(collector);
        outlierMad.eval(values, timestampArrayList, getLocalDateTimeFromEpoch(0L), 15, 4, 3, 76);

        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple1 = new Tuple5<>(new Timestamp(660000L), 5D, 5D, 5D, false);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple2 = new Tuple5<>(new Timestamp(720000L), 5D, 5D, 5D, false);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple3 = new Tuple5<>(new Timestamp(780000L), 5D, 5D, 5D, false);
        Tuple5<Timestamp, Double, Double, Double, Boolean> expectedTuple4 = new Tuple5<>(new Timestamp(840000L), 5D, 5D, 5D, false);

        Mockito.verify(collector, times(4)).collect(tuple5ArgumentCaptor.capture());
        List<Tuple5<Timestamp, Double, Double, Double, Boolean>> allTuples = tuple5ArgumentCaptor.getAllValues();

        Assert.assertEquals(expectedTuple1, allTuples.get(0));
        Assert.assertEquals(expectedTuple2, allTuples.get(1));
        Assert.assertEquals(expectedTuple3, allTuples.get(2));
        Assert.assertEquals(expectedTuple4, allTuples.get(3));
    }


    private LocalDateTime getLocalDateTimeFromEpoch(long epoch) {
        return Instant.ofEpochMilli(epoch).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }
}
