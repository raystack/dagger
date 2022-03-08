package io.odpf.dagger.common.serde.parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Objects;

public class SimpleGroupValidation {
    public boolean checkSimpleGroupNotNull(SimpleGroup simpleGroup) {
        return !Objects.isNull(simpleGroup);
    }

    public boolean checkFieldIsPrimitive(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroup.getType().getType(fieldName).isPrimitive();
    }

    public boolean checkFieldExistsInSimpleGroup(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroup.getType().containsField(fieldName);
    }

    public boolean checkLogicalTypeIsSupported(SimpleGroup simpleGroup, String fieldName, LogicalTypeAnnotation supportedLogicalTypeAnnotation) {
        LogicalTypeAnnotation fieldLogicalTypeAnnotation = simpleGroup.getType()
                .getType(fieldName)
                .getLogicalTypeAnnotation();
        return Objects.equals(supportedLogicalTypeAnnotation, fieldLogicalTypeAnnotation);
    }

    public boolean checkParserCanParsePrimitive(SimpleGroup simpleGroup, String fieldName, PrimitiveType.PrimitiveTypeName supportedPrimitiveTypeName) {
        PrimitiveType.PrimitiveTypeName fieldPrimitiveType = simpleGroup.getType()
                .getType(fieldName)
                .asPrimitiveType()
                .getPrimitiveTypeName();
        return Objects.equals(supportedPrimitiveTypeName, fieldPrimitiveType);
    }

    public boolean applyValidations(SimpleGroup simpleGroup,
                                    String fieldName,
                                    PrimitiveType.PrimitiveTypeName supportedPrimitiveTypeName,
                                    LogicalTypeAnnotation supportedLogicalTypeAnnotation) {
        return this.checkSimpleGroupNotNull(simpleGroup)
                && this.checkFieldExistsInSimpleGroup(simpleGroup, fieldName)
                && this.checkFieldIsPrimitive(simpleGroup, fieldName)
                && this.checkParserCanParsePrimitive(simpleGroup, fieldName, supportedPrimitiveTypeName)
                && this.checkLogicalTypeIsSupported(simpleGroup, fieldName, supportedLogicalTypeAnnotation);
    }

    public static boolean checkFieldExistsAndIsInitialized(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroup.getType().containsField(fieldName) && simpleGroup.getFieldRepetitionCount(fieldName) != 0;
    }
}