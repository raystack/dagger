package io.odpf.dagger.common.serde.parquet.parser;

import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.function.Supplier;

public interface ParquetDataTypeParser {
    Object deserialize(SimpleGroup simpleGroup, String fieldName);

    static Object getValueOrDefault(SimpleGroup simpleGroup, Supplier<Object> valueSupplier, Object defaultValue) {
        return simpleGroup == null ? defaultValue : valueSupplier.get();
    }
}