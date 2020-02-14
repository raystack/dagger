package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class IntegerTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public IntegerTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.INT;
    }

    @Override
    public Object getValue(Object field) {
        return Integer.parseInt(getValueOrDefault(field, "0"));
    }
}
