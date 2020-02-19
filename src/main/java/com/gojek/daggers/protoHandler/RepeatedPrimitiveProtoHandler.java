package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

public class RepeatedPrimitiveProtoHandler implements ProtoHandler {
    private final FieldDescriptor fieldDescriptor;

    public RepeatedPrimitiveProtoHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.isRepeated() && fieldDescriptor.getJavaType() != MESSAGE;
    }

    @Override
    public DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Object field) {
        if (!canHandle() || field == null)
            return builder;
        if (field.getClass().isArray())
            field = Arrays.asList((Object[]) field);
        return builder.setField(fieldDescriptor, field);
    }

    @Override
    public Object transform(Object field) {
        ArrayList<Object> outputValues = new ArrayList<>();
        if (field != null) {
            List<Object> inputValues = (List<Object>) field;
            PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);
            for (Object inputField : inputValues) {
                outputValues.add(primitiveProtoHandler.transform(inputField));
            }
        }
        return outputValues;
    }
}
