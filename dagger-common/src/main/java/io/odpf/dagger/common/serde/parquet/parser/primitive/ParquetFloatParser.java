package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.PrimitiveType;

import java.util.function.Supplier;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;

public class ParquetFloatParser implements ParquetDataTypeParser {
    private static final float DEFAULT_DESERIALIZED_VALUE = 0.0f;
    private static final PrimitiveType.PrimitiveTypeName SUPPORTED_PRIMITIVE_TYPE = FLOAT;
    private final SimpleGroupValidation simpleGroupValidation;

    public ParquetFloatParser(SimpleGroupValidation simpleGroupValidation) {
        this.simpleGroupValidation = simpleGroupValidation;
    }

    @Override
    public boolean canHandle(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroupValidation.applyValidations(simpleGroup, fieldName, SUPPORTED_PRIMITIVE_TYPE, null);
    }

    @Override
    public Object deserialize(SimpleGroup simpleGroup, String fieldName) {
        Supplier<Object> valueSupplier = () -> {
            int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
            /* this checks if the field value is missing */
            if (simpleGroup.getFieldRepetitionCount(columnIndex) == 0) {
                return null;
            } else {
                return simpleGroup.getFloat(columnIndex, 0);
            }
        };
        return ParquetDataTypeParser.getValueOrDefault(valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }
}