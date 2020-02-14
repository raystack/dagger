package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class StringTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public StringTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.STRING;
    }

    @Override
    public Object getValue(Object field) {
        return getValueOrDefault(field, "");
    }
}
