package org.raystack.dagger.functions.udfs.scalar.longbow.array.expression;

/**
 * The Operation expression.
 */
public class OperationExpression implements Expression {
    public static final String CONVERT_TO_ARRAY = ".toArray()";
    private String expressionString;

    @Override
    public String getExpressionString() {
        return expressionString;
    }

    @Override
    public void createExpression(String operationType) {
        this.expressionString = BASE_STRING + getOperationExpression(operationType) + CONVERT_TO_ARRAY;
    }
}
