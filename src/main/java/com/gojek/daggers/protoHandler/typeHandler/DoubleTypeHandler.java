package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class DoubleTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public DoubleTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.DOUBLE;
    }

    @Override
    public Object getValue(Object field) {
        return Double.parseDouble(getValueOrDefault(field, "0"));
    }
}
