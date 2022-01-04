package io.odpf.dagger.functions.udfs.scalar.longbow.array.processors;

import io.odpf.dagger.functions.exceptions.ArrayAggregationException;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.LongbowArrayType;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.expression.AggregationExpression;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.OptionalInt;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ArrayAggregateProcessorTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ArrayAggregateProcessor arrayAggregateProcessor;

    @Mock
    private JexlEngine jexlEngine;

    @Mock
    private JexlContext jexlContext;

    @Mock
    private JexlScript jexlScript;

    @Mock
    private AggregationExpression aggregationExpression;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAggregateObjectArray() {
        when(aggregationExpression.getExpressionString()).thenReturn("test-expression");
        when(jexlEngine.createScript("test-expression")).thenReturn(jexlScript);
        arrayAggregateProcessor = new ArrayAggregateProcessor(jexlEngine, jexlContext, jexlScript, aggregationExpression);
        Object[] objects = new Object[1];
        objects[0] = 1L;
        arrayAggregateProcessor.initJexl(LongbowArrayType.BIGINT, objects);
        arrayAggregateProcessor.process();
        verify(jexlScript, times(1)).execute(jexlContext);
    }

    @Test
    public void shouldFlattenOptionalOutput() {
        OptionalInt optionalInt = OptionalInt.empty();
        when(aggregationExpression.getExpressionString()).thenReturn("test-expression");
        when(jexlEngine.createScript("test-expression")).thenReturn(jexlScript);
        when(jexlScript.execute(jexlContext)).thenReturn(optionalInt);
        arrayAggregateProcessor = new ArrayAggregateProcessor(jexlEngine, jexlContext, jexlScript, aggregationExpression);
        Object[] objects = new Object[1];
        objects[0] = 1L;
        arrayAggregateProcessor.initJexl(LongbowArrayType.BIGINT, objects);
        Object result = arrayAggregateProcessor.process();
        Assert.assertEquals(result, 0);
    }

    @Test
    public void shouldThrowLongbowExceptionIfExecutionFails() {
        thrown.expect(ArrayAggregationException.class);
        thrown.expectMessage("testing");

        when(aggregationExpression.getExpressionString()).thenReturn("test-expression");
        when(jexlEngine.createScript("test-expression")).thenReturn(jexlScript);
        when(jexlScript.execute(jexlContext)).thenThrow(new ClassCastException("testing"));
        arrayAggregateProcessor = new ArrayAggregateProcessor(jexlEngine, jexlContext, jexlScript, aggregationExpression);
        Object[] objects = new Object[1];
        objects[0] = 1L;
        arrayAggregateProcessor.initJexl(LongbowArrayType.BIGINT, objects);
        arrayAggregateProcessor.process();
    }
}
