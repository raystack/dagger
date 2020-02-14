package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class FloatTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public FloatTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.FLOAT;
    }

    @Override
    public Object getValue(Object field) {
        return Float.parseFloat(getValueOrDefault(field, "0"));
    }
}
