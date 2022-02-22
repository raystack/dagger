package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.function.Supplier;

public class ParquetInt32Parser implements ParquetDataTypeParser {
    private static final int DEFAULT_DESERIALIZED_VALUE = 0;

    @Override
    public Object deserialize(SimpleGroup simpleGroup, String fieldName) {
        Supplier<Object> valueSupplier = () -> {
            int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
            return simpleGroup.getInteger(columnIndex, 0);
        };
        return ParquetDataTypeParser.getValueOrDefault(simpleGroup, valueSupplier, DEFAULT_DESERIALIZED_VALUE);
    }
}