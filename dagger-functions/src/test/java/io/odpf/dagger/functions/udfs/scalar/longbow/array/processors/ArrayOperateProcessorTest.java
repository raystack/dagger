package io.odpf.dagger.functions.udfs.scalar.longbow.array.processors;


import io.odpf.dagger.functions.exceptions.ArrayOperateException;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.LongbowArrayType;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.expression.OperationExpression;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ArrayOperateProcessorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ArrayOperateProcessor arrayOperateProcessor;

    @Mock
    private JexlEngine jexlEngine;

    @Mock
    private JexlContext jexlContext;

    @Mock
    private JexlScript jexlScript;

    @Mock
    private OperationExpression operationExpression;

    @Before
    public void setup() {
        initMocks(this);
    }


    @Test
    public void shouldOperateOnObjectArray() {
        when(operationExpression.getExpressionString()).thenReturn("test-expression");
        when(jexlEngine.createScript("test-expression")).thenReturn(jexlScript);
        arrayOperateProcessor = new ArrayOperateProcessor(jexlEngine, jexlContext, jexlScript, operationExpression);
        Object[] objects = new Object[1];
        objects[0] = 1L;
        arrayOperateProcessor.initJexl(LongbowArrayType.BIGINT, objects);
        arrayOperateProcessor.process();
        verify(jexlScript, times(1)).execute(jexlContext);
    }


    @Test
    public void shouldThrowLongbowExceptionIfExecutionFails() {
        thrown.expect(ArrayOperateException.class);
        thrown.expectMessage("testing");

        when(operationExpression.getExpressionString()).thenReturn("test-expression");
        when(jexlEngine.createScript("test-expression")).thenReturn(jexlScript);
        when(jexlScript.execute(jexlContext)).thenThrow(new ClassCastException("testing"));
        arrayOperateProcessor = new ArrayOperateProcessor(jexlEngine, jexlContext, jexlScript, operationExpression);
        Object[] objects = new Object[1];
        objects[0] = 1L;
        arrayOperateProcessor.initJexl(LongbowArrayType.BIGINT, objects);
        arrayOperateProcessor.process();
    }
}
