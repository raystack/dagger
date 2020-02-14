package com.gojek.daggers.protoHandler;

import com.gojek.daggers.exception.InvalidDataTypeException;
import com.gojek.daggers.protoHandler.typeHandler.PrimitiveTypeHandlerFactory;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

public class RepeatedProtoHandler implements ProtoHandler {
    private final FieldDescriptor fieldDescriptor;

    public RepeatedProtoHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.isRepeated() && fieldDescriptor.getJavaType() != MESSAGE;
    }

    @Override
    public DynamicMessage.Builder getProtoBuilder(DynamicMessage.Builder builder, Object field) {
        return builder.setField(fieldDescriptor, field);
    }

    @Override
    public Object getTypeAppropriateValue(Object field) {
        return getList((List<Object>) field, fieldDescriptor);
    }

    private static Object getList(List<Object> inputValues, FieldDescriptor fieldDescriptor) {
        ArrayList<Object> outputValues = new ArrayList<>();
        for (Object field : inputValues) {
            try {
                outputValues.add(PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor).getValue(field));
            } catch (NumberFormatException e) {
                String errMessage = String.format("type mismatch of field: %s, expecting %s type, actual type %s", fieldDescriptor.getName(), fieldDescriptor.getType(), field.getClass());
                throw new InvalidDataTypeException(errMessage);
            }
        }
        return outputValues;
    }
}
