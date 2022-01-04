package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;

import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.LongbowArrayType;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.expression.AggregationExpression;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayAggregateProcessor;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.processors.ArrayProcessor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
        LongbowArrayType longbowArrayType = LongbowArrayType.getDataType(inputDataType);
        arrayProcessor.initJexl(longbowArrayType, arrayElements);
        return arrayProcessor.process();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        TypeInference build = TypeInference.newBuilder()
                .inputTypeStrategy(new ArrayAggregateInputStrategy())
                .outputTypeStrategy(new ArrayAggregateOutputStrategy())
                .build();
        return build;
    }


    private static class ArrayAggregateOutputStrategy implements TypeStrategy {
        @Override
        public Optional<org.apache.flink.table.types.DataType> inferType(CallContext callContext) {
            DataTypeFactory dataTypeFactory = callContext.getDataTypeFactory();
            UnresolvedDataType opUnresolvedType = DataTypes.RAW(new GenericTypeInfo<>(Object.class));
            DataType opDataType = dataTypeFactory.createDataType(opUnresolvedType);
            return Optional.of(opDataType);
        }
    }

    private static class ArrayAggregateInputStrategy implements InputTypeStrategy {
        private static final Integer ARRAY_AGGREGATE_UDF_FUNCTION_ARG_COUNT = 3;

        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.of(ARRAY_AGGREGATE_UDF_FUNCTION_ARG_COUNT);
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
            DataTypeFactory dataTypeFactory = callContext.getDataTypeFactory();
            UnresolvedDataType unresolvedArgOneType = DataTypes.ARRAY(DataTypes.RAW(new GenericTypeInfo<>(Object.class)));
            DataType resolvedArgOneType = dataTypeFactory.createDataType(unresolvedArgOneType);
            return Optional.of(Arrays.asList(new org.apache.flink.table.types.DataType[]{resolvedArgOneType, DataTypes.STRING(), DataTypes.STRING()}));
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return null;
        }
    }
}
