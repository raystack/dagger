package io.odpf.dagger.common.serde.parquet.parser.primitive;

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
            LogicalTypeAnnotation logicalTypeAnnotation = simpleGroup.getType().getType(columnIndex).getLogicalTypeAnnotation();
            checkLogicalTypeAnnotation(logicalTypeAnnotation);
            return simpleGroup.getString(columnIndex, 0);
        };
        return ParquetDataTypeParser.getValueOrDefault(simpleGroup, valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }

    private void checkLogicalTypeAnnotation(LogicalTypeAnnotation logicalTypeAnnotation) {
        if (logicalTypeAnnotation != SUPPORTED_LOGICAL_TYPE_ANNOTATION) {
            throw new ClassCastException("Error: Expected logical type as STRING but found " + logicalTypeAnnotation.toString());
        }
    }
}
