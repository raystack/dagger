package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.functions.exceptions.ArrayOperateException;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.lang.reflect.Array;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ArrayOperateTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "ArrayOperate")).thenReturn(metricGroup);
    }

    @Test
    public void shouldComputeBasicOPerationsForArray() throws Exception {
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = "v";
        objects[4] = "a";
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        Object[] result = arrayOperate.eval(objects, "distinct", "other");
        Assert.assertEquals(3, result.length);
        Assert.assertEquals("a", Array.get(result, 0));
        Assert.assertEquals("b", Array.get(result, 1));
        Assert.assertEquals("v", Array.get(result, 2));
    }

    @Test
    public void shouldComputeBasicArrayOperationsForIntArray() throws Exception {
        Object[] objects = new Object[5];
        objects[0] = 1;
        objects[1] = 2;
        objects[2] = 1;
        objects[3] = 2;
        objects[4] = 1;
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        Object[] result = arrayOperate.eval(objects, "distinct.sorted", "int");
        Assert.assertEquals(2, result.length);
        Assert.assertEquals(1, Array.get(result, 0));
        Assert.assertEquals(2, Array.get(result, 1));
    }

    @Test
    public void shouldComputeBasicArrayOperationsForDoubleArray() throws Exception {
        Object[] objects = new Object[5];
        objects[0] = 1.3d;
        objects[1] = 2.1d;
        objects[2] = 1.3d;
        objects[3] = 0.1d;
        objects[4] = 1.3d;
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        Object[] result = arrayOperate.eval(objects, "distinct.sorted", "double");
        Assert.assertEquals(3, result.length);
        Assert.assertEquals(0.1d, Array.get(result, 0));
        Assert.assertEquals(1.3d, Array.get(result, 1));
    }

    @Test
    public void shouldThrowErrorIfFunctionIsUnsupported() throws Exception {
        thrown.expect(ArrayOperateException.class);
        thrown.expectMessage("org.apache.commons.jexl3.JexlException$Method: io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayProcessor.initJexl@1:18 unsolvable function/method 'sort'");
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = "v";
        objects[4] = "a";
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        arrayOperate.eval(objects, "distinct.sort", "other");
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
