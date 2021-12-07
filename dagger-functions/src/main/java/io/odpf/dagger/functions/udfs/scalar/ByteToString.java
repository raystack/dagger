package io.odpf.dagger.functions.udfs.scalar;

import com.google.protobuf.ByteString;
import io.odpf.dagger.common.udfs.ScalarUdf;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ByteToString extends ScalarUdf {
    /**
     * Given a ByteString, this UDF converts to String.
     *
     * @param byteField the field with byte[] in proto
     * @return string value of byteField
     */
    public String eval(ByteString byteField) {
        return byteField.toStringUtf8();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        TypeInference build = TypeInference.newBuilder()
                .inputTypeStrategy(new ByteStringInputStrategy()).outputTypeStrategy(new ByteStringOutputStrategy())
                .build();
        return build;
    }

    private static class ByteStringOutputStrategy implements TypeStrategy {
        @Override
        public Optional<DataType> inferType(CallContext callContext) {
            return Optional.of(DataTypes.STRING());
        }
    }

    private static class ByteStringInputStrategy implements InputTypeStrategy {
        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.of(1);
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
            DataType dataType = callContext.getArgumentDataTypes().get(0);
            return Optional.of(Arrays.asList(dataType));
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return null;
        }
    }
}
