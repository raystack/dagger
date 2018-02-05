package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

public class EnumProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public EnumProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canPopulate() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM;
    }

    @Override
    public DynamicMessage.Builder populate(DynamicMessage.Builder builder, Object field) {
        if (!canPopulate()) {
            return builder;
        }
        return builder.setField(fieldDescriptor, fieldDescriptor.getEnumType().findValueByName(String.valueOf(field)));
    }
}
