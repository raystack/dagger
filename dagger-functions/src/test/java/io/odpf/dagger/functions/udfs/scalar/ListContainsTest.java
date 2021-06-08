package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ListContainsTest {

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "ListContains")).thenReturn(metricGroup);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        ListContains listContains = new ListContains();
        listContains.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldReturnTrueIfListContainsTheItem() {
        ListContains listContains = new ListContains();
        String[] inputArray = {"test1", "test2"};
        Assert.assertTrue(listContains.eval(inputArray, "test1"));
    }

    @Test
    public void shouldReturnFalseIfListDoesNotContainTheItem() {
        ListContains listContains = new ListContains();
        String[] inputArray = {"test1", "test2"};
        Assert.assertFalse(listContains.eval(inputArray, "test3"));
    }

    @Test
    public void shouldReturnFalseForNullItem() {
        ListContains listContains = new ListContains();
        String[] inputArray = {"test1", "test2"};
        Assert.assertFalse(listContains.eval(inputArray, null));
    }

    @Test
    public void shouldReturnFalseForNullList() {
        ListContains listContains = new ListContains();
        Assert.assertFalse(listContains.eval(null, "item1"));
    }

    @Test
    public void shouldReturnFalseForEmptyList() {
        ListContains listContains = new ListContains();
        Assert.assertFalse(listContains.eval(new String[]{}, "item1"));
    }

}
