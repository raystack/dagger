package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.PrimitiveType;

import java.util.function.Supplier;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;

public class ParquetDoubleParser implements ParquetDataTypeParser {
    private static final double DEFAULT_DESERIALIZED_VALUE = 0.0d;
    private static final PrimitiveType.PrimitiveTypeName SUPPORTED_PRIMITIVE_TYPE = DOUBLE;
    private final SimpleGroupValidation simpleGroupValidation;

    public ParquetDoubleParser(SimpleGroupValidation simpleGroupValidation) {
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
            if(simpleGroup.getFieldRepetitionCount(columnIndex) == 0) {
                return null;
            } else {
                return simpleGroup.getDouble(columnIndex, 0);
            }
        };
        return ParquetDataTypeParser.getValueOrDefault(valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }
}