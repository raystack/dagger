package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

public class DefaultProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public DefaultProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canPopulate() {
        return true;
    }

    @Override
    public DynamicMessage.Builder populate(DynamicMessage.Builder builder, Object field) {
        return builder.setField(fieldDescriptor, field);
    }
}
