package io.odpf.dagger.functions.udfs.scalar.longbow.array.expression;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class AggregationExpressionTest {

    private AggregationExpression aggregationExpression;

    @Before
    public void setup() {
        initMocks(this);
        aggregationExpression = new AggregationExpression();
    }

    @Test
    public void shouldCreateExpressionForSingleExpression() {
        String operationType = "distinct";
        aggregationExpression.createExpression(operationType);

        Assert.assertEquals(aggregationExpression.getExpressionString(), "stream.distinct()");
    }

    @Test
    public void shouldCreateExpressionForMultipleExpression() {
        String operationType = "distinct.sorted.sum";
        aggregationExpression.createExpression(operationType);

        Assert.assertEquals(aggregationExpression.getExpressionString(), "stream.distinct().sorted().sum()");
    }
}
