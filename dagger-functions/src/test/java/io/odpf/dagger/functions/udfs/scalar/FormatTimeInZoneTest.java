package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FormatTimeInZoneTest {

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "FormatTimeInZone")).thenReturn(metricGroup);
    }

    @Test
    public void shouldReturnCorrectTimeInHHMMFormat() {
        long timeInSec = 1607003114;
        Timestamp timeStamp = new Timestamp(timeInSec * 1000);
        FormatTimeInZone timeInZone = new FormatTimeInZone();
        String expectedFormattedTime = "07:15 PM";
        Assert.assertEquals(expectedFormattedTime, timeInZone.eval(timeStamp, "Asia/Kolkata", "hh:mm aa"));
    }

    @Test
    public void shouldReturnCorrectTimeInHHMMFormatAndDifferentTZ() {
        long timeInSec = 1607003114;
        Timestamp timeStamp = new Timestamp(timeInSec * 1000);
        FormatTimeInZone timeInZone = new FormatTimeInZone();
        String expectedFormattedTime = "08:45 PM";
        Assert.assertEquals(expectedFormattedTime, timeInZone.eval(timeStamp, "Asia/Jakarta", "hh:mm aa"));
    }

    @Test
    public void shouldReturnCorrectTimeInYYYYMMDDFormat() {
        long timeInSec = 1607003114;
        Timestamp timeStamp = new Timestamp(timeInSec * 1000);
        FormatTimeInZone timeInZone = new FormatTimeInZone();
        String expectedFormattedTime = "2020/12/03";
        Assert.assertEquals(expectedFormattedTime, timeInZone.eval(timeStamp, "Asia/Kolkata", "yyyy/MM/dd"));
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        FormatTimeInZone timeInZone = new FormatTimeInZone();
        timeInZone.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
