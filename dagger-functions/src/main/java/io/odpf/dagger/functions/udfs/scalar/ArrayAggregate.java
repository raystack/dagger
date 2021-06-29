package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.DataType;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.expression.AggregationExpression;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayAggregateProcessor;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayProcessor;
import org.apache.flink.table.functions.FunctionContext;

import java.io.Serializable;

/**
 * The ArrayAggregate udf.
 */
public class ArrayAggregate extends ScalarUdf implements Serializable {
    private ArrayProcessor arrayProcessor;
    private AggregationExpression expression;

    /**
     * Instantiates a new Array aggregate.
     */
    public ArrayAggregate() {
        this.expression = new AggregationExpression();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        if (arrayProcessor == null) {
            arrayProcessor = new ArrayAggregateProcessor(expression);
        }
    }

    /**
     * Given an Object Array, this UDF performs basic mathematical functions on the Array.
     *
     * @param arrayElements input object array
     * @param operationType the aggregation function in string
     * @param inputDataType data type of object in the given array
     * @return the result of aggregate
     * @author arujit
     * @team DE
     */
    public Object eval(Object[] arrayElements, String operationType, String inputDataType) {
        expression.createExpression(operationType);
        DataType dataType = DataType.getDataType(inputDataType);
        arrayProcessor.initJexl(dataType, arrayElements);
        return arrayProcessor.process();
    }
}
