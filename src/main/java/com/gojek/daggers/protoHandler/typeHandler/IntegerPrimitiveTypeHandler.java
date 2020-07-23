package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.List;

public class IntegerPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public IntegerPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
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

    @Override
    public Object getArray(Object field) {
        List<Integer> inputValues = (List<Integer>) field;
        return inputValues.toArray(new Integer[]{});
    }
}
