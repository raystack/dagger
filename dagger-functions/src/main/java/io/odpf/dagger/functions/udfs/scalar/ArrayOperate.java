package io.odpf.dagger.functions.udfs.scalar;


import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.DataType;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.expression.OperationExpression;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayOperateProcessor;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayProcessor;
import org.apache.flink.table.functions.FunctionContext;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.stream.IntStream;

/**
 * The ArrayOperate udf.
 */
public class ArrayOperate extends ScalarUdf implements Serializable {
    private String expressionString;
    private ArrayProcessor arrayProcessor;
    private OperationExpression expression;

    /**
     * Instantiates a new Array operate.
     */
    public ArrayOperate() {
        this.expression = new OperationExpression();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        if (arrayProcessor == null) {
            arrayProcessor = new ArrayOperateProcessor(expression);
        }
    }

    /**
     * Given an Object Array, this UDF performs basic functions on the Array.
     *
     * @param arrayElements the array elements
     * @param operationType the operation type
     * @param inputDataType the input data type
     * @return the result of the aggregate
     */
    public Object[] eval(Object[] arrayElements, String operationType, String inputDataType) {
        expression.createExpression(operationType);
        DataType dataType = DataType.getDataType(inputDataType);
        arrayProcessor.initJexl(dataType, arrayElements);
        return getCopyArray(arrayProcessor.process());
    }

    private Object[] getCopyArray(Object originalArray) {
        int arrayLen = Array.getLength(originalArray);
        return IntStream.range(0, arrayLen).mapToObj(i -> Array.get(originalArray, i)).toArray();
    }
}
