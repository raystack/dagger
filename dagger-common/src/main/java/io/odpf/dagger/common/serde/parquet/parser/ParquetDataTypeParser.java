package io.odpf.dagger.common.serde.parquet.parser;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;

public interface ParquetDataTypeParser {
    Object deserialize(SimpleGroup simpleGroup, String fieldName);
    Type serialize(Object javaObject);

}