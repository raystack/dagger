package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.util.function.Supplier;

public class ParquetBinaryStringParser implements ParquetDataTypeParser {
    private static final String DEFAULT_DESERIALIZED_VALUE = "";
    private static final LogicalTypeAnnotation SUPPORTED_LOGICAL_TYPE_ANNOTATION = LogicalTypeAnnotation.stringType();

    @Override
    public Object deserialize(SimpleGroup simpleGroup, String fieldName) {
        Supplier<Object> valueSupplier = () -> {
            int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
            checkLogicalTypeAnnotation(simpleGroup, columnIndex);
            return simpleGroup.getString(columnIndex, 0);
        };
        return ParquetDataTypeParser.getValueOrDefault(simpleGroup, valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }

    private void checkLogicalTypeAnnotation(SimpleGroup simpleGroup, int columnIndex) {
        LogicalTypeAnnotation logicalTypeAnnotation = simpleGroup.getType().getType(columnIndex).getLogicalTypeAnnotation();
        if(logicalTypeAnnotation == null) {
            throw new DaggerDeserializationException("Error: Expected logical type as " + SUPPORTED_LOGICAL_TYPE_ANNOTATION + " but no annotation was found.");
        }
        if (logicalTypeAnnotation != SUPPORTED_LOGICAL_TYPE_ANNOTATION) {
            throw new DaggerDeserializationException("Error: Expected logical type as " + SUPPORTED_LOGICAL_TYPE_ANNOTATION + " but found " + logicalTypeAnnotation);
        }
    }
}
