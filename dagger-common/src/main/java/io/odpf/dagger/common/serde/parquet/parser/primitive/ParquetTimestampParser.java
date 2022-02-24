package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.function.Supplier;

public class ParquetTimestampParser implements ParquetDataTypeParser {
    private static final Timestamp DEFAULT_DESERIALIZED_VALUE = Timestamp.from(Instant.EPOCH);
    private static final LogicalTypeAnnotation SUPPORTED_LOGICAL_TYPE_ANNOTATION = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);

    @Override
    public Object deserialize(SimpleGroup simpleGroup, String fieldName) {
        Supplier<Object> valueSupplier = () -> {
            int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
            checkLogicalTypeAnnotation(simpleGroup, columnIndex);
            long millis = simpleGroup.getLong(columnIndex, 0);
            return Timestamp.from(Instant.ofEpochMilli(millis));
        };
        return ParquetDataTypeParser.getValueOrDefault(simpleGroup, valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }

    private void checkLogicalTypeAnnotation(SimpleGroup simpleGroup, int columnIndex) {
        LogicalTypeAnnotation logicalTypeAnnotation = simpleGroup.getType().getType(columnIndex).getLogicalTypeAnnotation();
        if (logicalTypeAnnotation == null) {
            throw new DaggerDeserializationException("Error: Expected logical type as " + SUPPORTED_LOGICAL_TYPE_ANNOTATION + " but no annotation was found.");
        } else if (!logicalTypeAnnotation.equals(SUPPORTED_LOGICAL_TYPE_ANNOTATION)) {
            throw new DaggerDeserializationException("Error: Expected logical type as " + SUPPORTED_LOGICAL_TYPE_ANNOTATION + " but found " + logicalTypeAnnotation);
        }
    }
}