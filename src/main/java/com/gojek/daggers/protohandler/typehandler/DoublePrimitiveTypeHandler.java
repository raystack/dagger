package com.gojek.daggers.protohandler.typehandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
import java.util.List;

public class DoublePrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public DoublePrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.DOUBLE;
    }

    @Override
    public Object getValue(Object field) {
        return Double.parseDouble(getValueOrDefault(field, "0"));
    }

    @Override
    public Object getArray(Object field) {
        List<Double> inputValues = new ArrayList<>();
        if (field != null) inputValues = (List<Double>) field;
        return inputValues.toArray(new Double[]{});
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.DOUBLE;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.PRIMITIVE_ARRAY(Types.DOUBLE);
    }
}
