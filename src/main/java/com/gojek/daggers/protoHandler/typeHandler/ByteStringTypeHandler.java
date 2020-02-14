package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class ByteStringTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public ByteStringTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.BYTE_STRING;
    }

    @Override
    public Object getValue(Object field) {
        throw new RuntimeException("BYTE_STRING is not supported yet");
    }
}
