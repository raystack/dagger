package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.util.function.Supplier;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class ParquetBinaryStringParser implements ParquetDataTypeParser {
    private static final String DEFAULT_DESERIALIZED_VALUE = "";
    private static final PrimitiveType.PrimitiveTypeName SUPPORTED_PRIMITIVE_TYPE = BINARY;
    private static final LogicalTypeAnnotation SUPPORTED_LOGICAL_TYPE_ANNOTATION = LogicalTypeAnnotation.stringType();
    private final SimpleGroupValidation simpleGroupValidation;

    public ParquetBinaryStringParser(SimpleGroupValidation simpleGroupValidation) {
        this.simpleGroupValidation = simpleGroupValidation;
    }

    @Override
    public boolean canHandle(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroupValidation.applyValidations(simpleGroup, fieldName, SUPPORTED_PRIMITIVE_TYPE, SUPPORTED_LOGICAL_TYPE_ANNOTATION);
    }

    @Override
    public Object deserialize(SimpleGroup simpleGroup, String fieldName) {
        Supplier<Object> valueSupplier = () -> {
            int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
            /* this checks if the field value is missing */
            if (simpleGroup.getFieldRepetitionCount(columnIndex) == 0) {
                return null;
            } else {
                return simpleGroup.getString(columnIndex, 0);
            }
        };
        return ParquetDataTypeParser.getValueOrDefault(valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }
}