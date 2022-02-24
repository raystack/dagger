package io.odpf.dagger.common.serde.parquet.parser.validation;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Objects;

public class SimpleGroupValidationFactory {

    public static boolean checkSimpleGroupNotNull(SimpleGroup simpleGroup) {
      return !Objects.isNull(simpleGroup);
    }

    public static boolean checkFieldIsPrimitive(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroup.getType().getType(fieldName).isPrimitive();
    }

    public static boolean checkFieldExistsInSimpleGroup(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroup.getType().containsField(fieldName);
    }

    public static boolean checkLogicalTypeIsSupported(SimpleGroup simpleGroup, String fieldName, LogicalTypeAnnotation supportedLogicalTypeAnnotation) {
        int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
        return simpleGroup.getType()
                .getType(columnIndex)
                .getLogicalTypeAnnotation()
                .equals(supportedLogicalTypeAnnotation);
    }

    public static boolean checkPrimitiveTypeIsSupported(SimpleGroup simpleGroup, String fieldName, PrimitiveType.PrimitiveTypeName supportedPrimitiveTypeName) {
        int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
        return simpleGroup.getType()
                .getType(columnIndex)
                .asPrimitiveType()
                .getPrimitiveTypeName()
                .equals(supportedPrimitiveTypeName);
    }
}