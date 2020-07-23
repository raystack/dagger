package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.List;

public class StringPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public StringPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
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

    @Override
    public Object getArray(Object field) {
        List<String> inputValues = (List<String>) field;
        return inputValues.toArray(new String[]{});
    }
}
