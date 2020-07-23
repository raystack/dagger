package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

public class StructMessageProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public StructMessageProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE &&
                fieldDescriptor.toProto().getTypeName().equals(".google.protobuf.Struct");
    }

    @Override
    public DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Object field) {
        return builder;
    }

    @Override
    public Object transformForPostProcessor(Object field) {
        return null;
    }

    @Override
    public Object transformForKafka(Object field) {
        return null;
    }
}
