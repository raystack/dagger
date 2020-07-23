package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.List;

public class FloatPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public FloatPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
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

    @Override
    public Object getArray(Object field) {
        List<Float> inputValues = (List<Float>) field;
        return inputValues.toArray(new Float[]{});
    }
}
