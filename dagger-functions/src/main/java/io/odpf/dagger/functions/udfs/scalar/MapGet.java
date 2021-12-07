package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The Map get udf.
 */
public class MapGet extends ScalarUdf {
    /**
     * returns value for a corresponding key inside a map data type.
     *
     * @param inputMap the input map element
     * @param key      the key
     * @return the value
     * @author Ujjawal
     * @team Fraud
     */

    public Object eval(Row[] inputMap, Object key) {
        List<Row> rows = Arrays.asList(inputMap);
        Optional<Row> requiredRow = rows.stream().filter(row -> row.getField(0).equals(key)).findFirst();
        return requiredRow.map(row -> row.getField(1)).orElse(null);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(new MapGetInputTypeStrategy())
                .outputTypeStrategy(new MapOutputTypeStrategy())
                .build();
    }

    private static class MapOutputTypeStrategy implements TypeStrategy {
        @Override
        public Optional<DataType> inferType(CallContext callContext) {
            CollectionDataType firstArgumentDataType = (CollectionDataType) callContext.getArgumentDataTypes().get(0);
            FieldsDataType elementDataType = (FieldsDataType) firstArgumentDataType.getElementDataType();
            List<DataType> children = elementDataType.getChildren();
            return Optional.of(children.get(1));
        }
    }

    private static class MapGetInputTypeStrategy implements InputTypeStrategy {
        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.of(2);
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
            CollectionDataType firstArgumentDataType = (CollectionDataType) callContext.getArgumentDataTypes().get(0);
            FieldsDataType elementDataType = (FieldsDataType) firstArgumentDataType.getElementDataType();
            List<DataType> children = elementDataType.getChildren();
            AtomicDataType secondArgumentDataType = (AtomicDataType) callContext.getArgumentDataTypes().get(1);
            DataType mapDataType = DataTypes.ARRAY(DataTypes.ROW(children.get(0), children.get(1)));
            return Optional.of(Arrays.asList(mapDataType, secondArgumentDataType));
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return null;
        }
    }
}
