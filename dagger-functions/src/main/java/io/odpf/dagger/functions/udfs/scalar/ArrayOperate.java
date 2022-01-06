package io.odpf.dagger.functions.udfs.scalar;


import org.apache.flink.table.functions.FunctionContext;

import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.LongbowArrayType;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.expression.OperationExpression;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayOperateProcessor;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayProcessor;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Optional;
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
     * @param operationType the operation type
     * @param inputDataType the input data type
     * @param arrayElements the array elements
     * @return the result of the aggregate
     */

   public Object[] eval(String operationType, String inputDataType, @DataTypeHint(inputGroup = InputGroup.ANY) Object... arrayElements) {
        expression.createExpression(operationType);
        LongbowArrayType dataType = LongbowArrayType.getDataType(inputDataType);
        arrayProcessor.initJexl(dataType, arrayElements);
        return getCopyArray(arrayProcessor.process());
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        TypeInference build = TypeInference.newBuilder()
                .outputTypeStrategy(new TypeStrategy() {
                    @Override
                    public Optional<org.apache.flink.table.types.DataType> inferType(CallContext callContext) {
                        DataTypeFactory dataTypeFactory = callContext.getDataTypeFactory();
                        UnresolvedDataType unresolvedDataType = DataTypes.ARRAY(DataTypes.RAW(new GenericTypeInfo<>(Object.class)));
                        org.apache.flink.table.types.DataType dataType = dataTypeFactory.createDataType(unresolvedDataType);
                        return Optional.of(dataType);
                    }
                })
                .build();
        return build;
    }

    private Object[] getCopyArray(Object originalArray) {
        int arrayLen = Array.getLength(originalArray);
        return IntStream.range(0, arrayLen).mapToObj(i -> Array.get(originalArray, i)).toArray();
    }
}
