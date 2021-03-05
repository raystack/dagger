package com.gojek.daggers.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.gojek.daggers.exception.InvalidDataTypeException;
import com.gojek.daggers.protohandler.typehandler.PrimitiveTypeHandler;
import com.gojek.daggers.protohandler.typehandler.PrimitiveTypeHandlerFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

public class PrimitiveProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public PrimitiveProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return true;
    }

    @Override
    public DynamicMessage.Builder transformForKafka(DynamicMessage.Builder builder, Object field) {
        return field != null ? builder.setField(fieldDescriptor, field) : builder;
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor);
        try {
            return primitiveTypeHandler.getValue(field);
        } catch (NumberFormatException e) {
            String errMessage = String.format("type mismatch of field: %s, expecting %s type, actual type %s", fieldDescriptor.getName(), fieldDescriptor.getType(), field.getClass());
            throw new InvalidDataTypeException(errMessage);
        }
    }

    @Override
    public Object transformFromKafka(Object field) {
        return field;
    }

    @Override
    public Object transformToJson(Object field) {
        return field;
    }

    @Override
    public TypeInformation getTypeInformation() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor);
        return primitiveTypeHandler.getTypeInformation();
    }

}
