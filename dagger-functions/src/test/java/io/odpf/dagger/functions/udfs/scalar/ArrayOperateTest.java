package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.functions.exceptions.ArrayOperateException;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.ArrayMatching.arrayContaining;
import static org.hamcrest.collection.ArrayMatching.arrayContainingInAnyOrder;
import static org.junit.Assert.*;
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

    @Mock
    private CallContext callContext;

    @Mock
    private DataTypeFactory dataTypeFactory;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "ArrayOperate")).thenReturn(metricGroup);
    }

    @Test
    public void shouldComputeBasicOPerationsForArray() throws Exception {
        Object[] objects = new Object[] {"a", "a", "b", "v", "a"};
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        Object[] objectList = arrayOperate.eval(objects, "distinct", "other");
        assertThat(objectList, arrayContainingInAnyOrder("a", "b", "v"));
    }

    @Test
    public void shouldComputeBasicArrayOperationsForIntArray() throws Exception {
        Object[] objects = new Object[] {1, 2, 1, 2, 1};
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        Object[] result = arrayOperate.eval(objects, "distinct.sorted", "int");
        assertThat(result, arrayContaining(1, 2));
    }

    @Test
    public void shouldComputeBasicArrayOperationsForDoubleArray() throws Exception {
        Object[] objects = new Object[] {1.3d, 2.1d, 1.3d, 0.1d, 1.3d};
        ArrayOperate arrayOperate = new ArrayOperate();
        arrayOperate.open(functionContext);
        Object[] result = arrayOperate.eval(objects, "distinct.sorted", "double");
        assertThat(result, arrayContaining(0.1d, 1.3d, 2.1d));
    }

    @Test
    public void shouldThrowErrorIfFunctionIsUnsupported() throws Exception {
        thrown.expect(ArrayOperateException.class);
        thrown.expectMessage("org.apache.commons.jexl3.JexlException$Method: io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayProcessor.initJexl@1:18 unsolvable function/method 'sort'");
        Object[] objects = new Object[] {"a", "a", "b", "v", "a"};
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

    @Test
    public void shouldResolveInPutTypeStrategyForUnresolvedTypes() {
        when(callContext.getDataTypeFactory()).thenReturn(dataTypeFactory);
        InputTypeStrategy inputTypeStrategy = new ArrayOperate().getTypeInference(dataTypeFactory).getInputTypeStrategy();
        inputTypeStrategy.inferInputTypes(callContext, true);
        verify(dataTypeFactory, times(1)).createDataType(any(UnresolvedDataType.class));
    }

    @Test
    public void shouldRegisterThreeInputArguments() {
        when(callContext.getDataTypeFactory()).thenReturn(dataTypeFactory);
        InputTypeStrategy inputTypeStrategy = new ArrayOperate().getTypeInference(dataTypeFactory).getInputTypeStrategy();
        inputTypeStrategy.inferInputTypes(callContext, true);
        assertEquals(ConstantArgumentCount.of(3), inputTypeStrategy.getArgumentCount());
    }
}
