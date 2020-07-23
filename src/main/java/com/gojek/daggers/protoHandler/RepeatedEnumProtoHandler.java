package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;

public class RepeatedEnumProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public RepeatedEnumProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == ENUM && fieldDescriptor.isRepeated();
    }

    @Override
    public DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Object field) {
        return builder;
    }

    @Override
    public Object transformForPostProcessor(Object field) {
        return getStringRow((List) field);
    }

    @Override
    public Object transformForKafka(Object field) {
        return getStringRow((List) field);
    }

    private String[] getStringRow(List<Object> protos) {
        return protos
                .stream()
                .map(String::valueOf)
                .toArray(String[]::new);
    }
}
