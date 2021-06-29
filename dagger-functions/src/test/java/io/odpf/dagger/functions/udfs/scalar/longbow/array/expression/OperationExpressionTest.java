package io.odpf.dagger.functions.udfs.scalar.longbow.array.expression;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class OperationExpressionTest {

    private OperationExpression operationExpression;

    @Before
    public void setup() {
        initMocks(this);
        operationExpression = new OperationExpression();
    }

    @Test
    public void shouldCreateExpressionForSingleExpression() {
        String operationType = "distinct";
        operationExpression.createExpression(operationType);

        Assert.assertEquals(operationExpression.getExpressionString(), "stream.distinct().toArray()");
    }

    @Test
    public void shouldCreateExpressionForMultipleExpression() {
        String operationType = "distinct.sorted";
        operationExpression.createExpression(operationType);

        Assert.assertEquals(operationExpression.getExpressionString(), "stream.distinct().sorted().toArray()");
    }
}
