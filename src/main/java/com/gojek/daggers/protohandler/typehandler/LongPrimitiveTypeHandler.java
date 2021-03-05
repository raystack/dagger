package com.gojek.daggers.protohandler.typehandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
import java.util.List;

public class LongPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public LongPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
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

    @Override
    public Object getArray(Object field) {
        List<Long> inputValues = new ArrayList<>();
        if (field != null) inputValues = (List<Long>) field;
        return inputValues.toArray(new Long[]{});
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.LONG;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.OBJECT_ARRAY(Types.LONG);
    }
}
