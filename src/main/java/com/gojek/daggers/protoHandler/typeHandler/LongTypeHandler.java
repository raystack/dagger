package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class LongTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public LongTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.LONG;
    }

    @Override
    public Object getValue(Object field) {
        return Long.parseLong(getValueOrDefault(field, "0"));
    }
}
