package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class BooleanTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public BooleanTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.BOOLEAN;
    }

    @Override
    public Object getValue(Object field) {
        return Boolean.parseBoolean(getValueOrDefault(field, "false"));
    }
}
