package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.function.Supplier;

public class ParquetBooleanParser implements ParquetDataTypeParser {
    private static final boolean DEFAULT_DESERIALIZED_VALUE = false;

    @Override
    public Object deserialize(SimpleGroup simpleGroup, String fieldName) {
        Supplier<Object> valueSupplier = () -> {
            int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
            return simpleGroup.getBoolean(columnIndex, 0);
        };
        return ParquetDataTypeParser.getValueOrDefault(simpleGroup, valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }
}