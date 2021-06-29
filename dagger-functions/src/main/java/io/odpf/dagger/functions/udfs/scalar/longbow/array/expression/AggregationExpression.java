package io.odpf.dagger.functions.udfs.scalar.longbow.array.expression;

/**
 * The Aggregation expression.
 */
public class AggregationExpression implements Expression {
    private String expressionString;

    @Override
    public String getExpressionString() {
        return expressionString;
    }

    @Override
    public void createExpression(String operationType) {
        this.expressionString = BASE_STRING + getOperationExpression(operationType);
    }
}
