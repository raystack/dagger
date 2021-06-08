package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class TimestampFromUnixTest {

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "TimestampFromUnix")).thenReturn(metricGroup);
    }

    @Test
    public void shouldReturnCorrectDate() throws ParseException {
        SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        TimestampFromUnix timestampFromUnix = new TimestampFromUnix();
        long unixTimestamp = 1554101494;
        Timestamp expectedDate = new Timestamp(dateFormater.parse("2019-04-01 13:51:34 WIB").getTime());
        Assert.assertEquals(expectedDate, timestampFromUnix.eval(unixTimestamp));
    }

    @Test
    public void shouldReturnCorrectDateWhenGetMinusSeconds() throws ParseException {
        SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        TimestampFromUnix timestampFromUnix = new TimestampFromUnix();
        long unixTimestamp = -1554101494;
        Timestamp expectedDate = new Timestamp(dateFormater.parse("2019-04-01 13:51:34 WIB").getTime());
        Assert.assertEquals(expectedDate, timestampFromUnix.eval(unixTimestamp));
    }

    @Test
    public void shouldReturnCorrectDateWhenGetZeroSeconds() throws ParseException {
        SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        TimestampFromUnix timestampFromUnix = new TimestampFromUnix();
        long unixTimestamp = 0;
        Timestamp expectedDate = new Timestamp(dateFormater.parse("1970-01-01 07:00:00 WIB").getTime());
        Assert.assertEquals(expectedDate, timestampFromUnix.eval(unixTimestamp));
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        TimestampFromUnix timestampFromUnix = new TimestampFromUnix();
        timestampFromUnix.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
