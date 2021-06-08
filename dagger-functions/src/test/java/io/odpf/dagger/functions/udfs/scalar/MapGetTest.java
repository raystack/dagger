package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class MapGetTest {

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "MapGet")).thenReturn(metricGroup);
    }

    @Test
    public void shouldGetValueForValueForGivenKeyInGivenMap() {
        ArrayList<Row> rows = new ArrayList<>();
        Row row1 = new Row(2);
        String expectedValue = "0.4";
        row1.setField(0, "payment_switch");
        row1.setField(1, "PB-123456");
        Row row2 = new Row(2);
        row2.setField(0, "weighted_abuse_probability");
        row2.setField(1, expectedValue);
        String givenKey = "weighted_abuse_probability";

        rows.add(row1);
        rows.add(row2);


        MapGet mapGet = new MapGet();
        Object actualValue = mapGet.eval(rows.toArray(new Row[0]), givenKey);

        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void shouldGetIntValueForValueForGivenKeyInGivenMap() {
        ArrayList<Row> rows = new ArrayList<>();
        Row row1 = new Row(2);
        int expectedValue = 4;
        row1.setField(0, "payment_switch");
        row1.setField(1, "PB-123456");
        Row row2 = new Row(2);
        row2.setField(0, "weighted_abuse_probability");
        row2.setField(1, expectedValue);
        String givenKey = "weighted_abuse_probability";

        rows.add(row1);
        rows.add(row2);


        MapGet mapGet = new MapGet();
        Object actualValue = mapGet.eval(rows.toArray(new Row[0]), givenKey);

        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void shouldReturnNullIfValueIsNotPresentInGivenMap() {
        ArrayList<Row> rows = new ArrayList<>();
        Row row1 = new Row(2);
        int expectedValue = 4;
        row1.setField(0, "payment_switch");
        row1.setField(1, "PB-123456");
        Row row2 = new Row(2);
        row2.setField(0, "weighted_abuse_probability");
        row2.setField(1, expectedValue);
        String givenKey = "abuse_probability";

        rows.add(row1);
        rows.add(row2);

        MapGet valueForKeyInMap = new MapGet();
        Object actualValue = valueForKeyInMap.eval(rows.toArray(new Row[0]), givenKey);

        assertNull(actualValue);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        MapGet mapGet = new MapGet();
        mapGet.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
