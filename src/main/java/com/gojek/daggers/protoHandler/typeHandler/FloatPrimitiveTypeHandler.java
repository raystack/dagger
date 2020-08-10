package com.gojek.daggers.protoHandler.typeHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
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
        List<Float> inputValues = new ArrayList<>();
        if (field != null) inputValues = (List<Float>) field;
        return inputValues.toArray(new Float[]{});
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.FLOAT;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.PRIMITIVE_ARRAY(Types.FLOAT);
    }
}
