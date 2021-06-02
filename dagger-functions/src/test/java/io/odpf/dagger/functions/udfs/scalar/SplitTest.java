package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SplitTest {
    private Split splitUDF;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "Split")).thenReturn(metricGroup);
        splitUDF = new Split();
    }

    @Test
    public void shouldCollectSplittedString() {
        String inputString = "1234567890 0987654321 000000000";
        String[] arrayString = splitUDF.eval(inputString, " ");

        Assert.assertArrayEquals(arrayString, inputString.split(" "));

    }

    @Test
    public void shouldSetSeparatorToSpaceIfInputSeparatorIsEmptyString() {
        String inputString = "1234567890 0987654321 000000000";
        String[] arrayString = splitUDF.eval(inputString, "");

        Assert.assertArrayEquals(arrayString, inputString.split(" "));
    }

    @Test
    public void shouldCollectInputStringIfSeparatorNotContainInInputString() {
        String inputString = "1234567890 0987654321 000000000";
        String[] arrayString = splitUDF.eval(inputString, ",");

        Assert.assertArrayEquals(arrayString, inputString.split(","));
    }

    @Test
    public void shouldReturnEmptyArrayWhenInputStringIsNull() {
        String inputString = null;
        String[] arrayString = splitUDF.eval(inputString, ",");

        Assert.assertEquals(0, arrayString.length);
    }

    @Test
    public void shouldReturnEmptyArrayWhenDelimiterIsNull() {
        String delimiter = null;
        String[] arrayString = splitUDF.eval("1234567890 0987654321 000000000", delimiter);

        Assert.assertEquals(0, arrayString.length);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        splitUDF.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
