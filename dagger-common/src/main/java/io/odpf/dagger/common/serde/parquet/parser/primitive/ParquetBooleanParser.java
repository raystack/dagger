package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;

public class ParquetBooleanParser implements ParquetDataTypeParser {
    @Override
    public Object deserialize(SimpleGroup simpleGroup, String fieldName) {
        int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
        return simpleGroup.getBoolean(columnIndex, 0);
    }

    @Override
    public Type serialize(Object javaObject) {
        throw new UnsupportedOperationException("Serialization of Flink data type to Parquet data types " +
                "is not supported yet.");
    }
}
