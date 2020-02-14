package com.gojek.daggers.protoHandler;

import com.gojek.daggers.exception.InvalidDataTypeException;
import com.gojek.daggers.protoHandler.typeHandler.PrimitiveTypeHandler;
import com.gojek.daggers.protoHandler.typeHandler.PrimitiveTypeHandlerFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

public class DefaultProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public DefaultProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return true;
    }

    @Override
    public DynamicMessage.Builder getProtoBuilder(DynamicMessage.Builder builder, Object field) {
        return field != null ? builder.setField(fieldDescriptor, field) : builder;
    }

    @Override
    public Object getTypeAppropriateValue(Object field) {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor);
        try {
            return primitiveTypeHandler.getValue(field);
        } catch (NumberFormatException e) {
            String errMessage = String.format("type mismatch of field: %s, expecting %s type, actual type %s", fieldDescriptor.getName(), fieldDescriptor.getType(), field.getClass());
            throw new InvalidDataTypeException(errMessage);
        }
    }

}
