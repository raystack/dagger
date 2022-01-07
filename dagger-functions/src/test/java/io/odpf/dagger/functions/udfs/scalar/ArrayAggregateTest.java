package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;

import io.odpf.dagger.functions.exceptions.ArrayAggregationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ArrayAggregateTest {
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
        when(metricGroup.addGroup("udf", "ArrayAggregate")).thenReturn(metricGroup);
    }

    @Test
    public void shouldComputeAggregateForIntegerArray() throws Exception {
        Object[] objects = new Object[3];
        objects[0] = 1;
        objects[1] = 2;
        objects[2] = 3;
        ArrayAggregate arrayAggregate = new ArrayAggregate();
        arrayAggregate.open(functionContext);
        Object result = arrayAggregate.eval(objects, "average", "Integer");
        assertEquals(result, 2d);
    }

    @Test
    public void shouldComputeAggregateForDoubleArray() throws Exception {
        Object[] objects = new Object[3];
        objects[0] = 1.3d;
        objects[1] = 2.3d;
        objects[2] = 3.3d;
        ArrayAggregate arrayAggregate = new ArrayAggregate();
        arrayAggregate.open(functionContext);
        Object result = arrayAggregate.eval(objects, "average", "double");
        assertEquals(result, 2.3d);
    }

    @Test
    public void shouldComputeAggregateForLongArray() throws Exception {
        Object[] objects = new Object[3];
        objects[0] = 1L;
        objects[1] = 2L;
        objects[2] = 3L;
        ArrayAggregate arrayAggregate = new ArrayAggregate();
        arrayAggregate.open(functionContext);
        Object result = arrayAggregate.eval(objects, "average", "long");
        assertEquals(result, 2d);
    }

    @Test
    public void shouldComputeNestedAggregatesForArray() throws Exception {
        Object[] objects = new Object[5];
        objects[0] = 1L;
        objects[1] = 1L;
        objects[2] = 1L;
        objects[3] = 2L;
        objects[4] = 3L;
        ArrayAggregate arrayAggregate = new ArrayAggregate();
        arrayAggregate.open(functionContext);
        Object result = arrayAggregate.eval(objects, "distinct.average", "long");
        assertEquals(result, 2d);
    }

    @Test
    public void shouldComputeBasicAggregatesForArray() throws Exception {
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = "v";
        objects[4] = "a";
        ArrayAggregate arrayAggregate = new ArrayAggregate();
        arrayAggregate.open(functionContext);
        Object result = arrayAggregate.eval(objects, "distinct.count", "other");
        assertEquals(result, 3L);
    }

    @Test
    public void shouldThrowErrorIfFunctionIsUnsupported() throws Exception {
        thrown.expect(ArrayAggregationException.class);
        thrown.expectMessage("io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayProcessor.initJexl@1:18 unsolvable function/method 'coun'");
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = "v";
        objects[4] = "a";
        ArrayAggregate arrayAggregate = new ArrayAggregate();
        arrayAggregate.open(functionContext);
        arrayAggregate.eval(objects, "distinct.coun", "other");
    }

    @Test
    public void shouldThrowErrorIfInputDatatypeIsUnsupported() throws Exception {
        thrown.expect(ArrayAggregationException.class);
        thrown.expectMessage("No support for inputDataType: String.Please provide 'Other' as inputDataType instead.");
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = "v";
        objects[4] = "a";
        ArrayAggregate arrayAggregate = new ArrayAggregate();
        arrayAggregate.open(functionContext);
        arrayAggregate.eval(objects, "distinct.count", "String");
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        ArrayAggregate arrayAggregate = new ArrayAggregate();
        arrayAggregate.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldResolveInPutTypeStrategyForUnresolvedTypes() {
        when(callContext.getDataTypeFactory()).thenReturn(dataTypeFactory);
        InputTypeStrategy inputTypeStrategy = new ArrayAggregate().getTypeInference(dataTypeFactory).getInputTypeStrategy();
        inputTypeStrategy.inferInputTypes(callContext, true);
        verify(dataTypeFactory, times(1)).createDataType(any(UnresolvedDataType.class));
    }

    @Test
    public void shouldRegisterThreeInputArguments() {
        when(callContext.getDataTypeFactory()).thenReturn(dataTypeFactory);
        InputTypeStrategy inputTypeStrategy = new ArrayAggregate().getTypeInference(dataTypeFactory).getInputTypeStrategy();
        inputTypeStrategy.inferInputTypes(callContext, true);
        assertEquals(ConstantArgumentCount.of(3), inputTypeStrategy.getArgumentCount());
    }
}
