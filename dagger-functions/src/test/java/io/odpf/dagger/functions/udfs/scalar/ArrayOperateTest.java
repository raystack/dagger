package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.functions.exceptions.ArrayOperateException;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInRelativeOrder.containsInRelativeOrder;
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
        List<Object> objectList = arrayOperate.eval("distinct", "other", objects);
        assertThat(objectList, containsInAnyOrder("a", "b", "v"));
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
        List<Object> result = arrayOperate.eval( "distinct.sorted", "int", objects);
        assertThat(result, containsInRelativeOrder(1, 2));
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
        List<Object> result = arrayOperate.eval( "distinct.sorted", "double", objects);
        assertThat(result, containsInRelativeOrder( 0.1d, 1.3d, 2.1d));
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
        arrayOperate.eval( "distinct.sort", "other", objects);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
