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
        return field != null ? builder.setField(fieldDescriptor, field) : builder;
    }

    @Override
    public Object getTypeAppropriateValue(Object field) {
        ArrayList<Object> outputValues = new ArrayList<>();
        if (field != null) {
            List<Object> inputValues = (List<Object>) field;
            for (Object inputField : inputValues) {
                try {
                    outputValues.add(PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor).getValue(inputField));
                } catch (NumberFormatException e) {
                    String errMessage = String.format("type mismatch of inputField: %s, expecting %s type, actual type %s",
                            fieldDescriptor.getName(), fieldDescriptor.getType(), inputField.getClass());
                    throw new InvalidDataTypeException(errMessage);
                }
            }
        }
        return outputValues;
    }
}
