package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.function.Supplier;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class ParquetTimestampParser implements ParquetDataTypeParser {
    private static final Timestamp DEFAULT_DESERIALIZED_VALUE = Timestamp.from(Instant.EPOCH);
    private static final PrimitiveType.PrimitiveTypeName SUPPORTED_PRIMITIVE_TYPE = INT64;
    private static final LogicalTypeAnnotation SUPPORTED_LOGICAL_TYPE_ANNOTATION = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
    private final SimpleGroupValidation simpleGroupValidation;

    public ParquetTimestampParser(SimpleGroupValidation simpleGroupValidation) {
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
                long millis = simpleGroup.getLong(columnIndex, 0);
                return Timestamp.from(Instant.ofEpochMilli(millis));
            }
        };
        return ParquetDataTypeParser.getValueOrDefault(valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }
}