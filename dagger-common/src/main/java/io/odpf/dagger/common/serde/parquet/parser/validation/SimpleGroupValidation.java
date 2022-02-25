package io.odpf.dagger.common.serde.parquet.parser.validation;

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
        int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
        LogicalTypeAnnotation fieldLogicalTypeAnnotation = simpleGroup.getType()
                .getType(columnIndex)
                .getLogicalTypeAnnotation();
        return supportedLogicalTypeAnnotation == fieldLogicalTypeAnnotation;
    }

    public boolean checkParserCanParsePrimitive(SimpleGroup simpleGroup, String fieldName, PrimitiveType.PrimitiveTypeName supportedPrimitiveTypeName) {
        int columnIndex = simpleGroup.getType().getFieldIndex(fieldName);
        return simpleGroup.getType()
                .getType(columnIndex)
                .asPrimitiveType()
                .getPrimitiveTypeName()
                .equals(supportedPrimitiveTypeName);
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
}