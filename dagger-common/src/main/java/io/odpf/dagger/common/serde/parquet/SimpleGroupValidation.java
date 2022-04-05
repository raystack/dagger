package io.odpf.dagger.common.serde.parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;

public class SimpleGroupValidation {
    public static boolean checkFieldExistsAndIsInitialized(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroup.getType().containsField(fieldName) && simpleGroup.getFieldRepetitionCount(fieldName) != 0;
    }
}
