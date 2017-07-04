package com.gojek.daggers;

import com.google.protobuf.Descriptors;

public class ProtoType {
    public static final String PROTO_CLASS_MISCONFIGURED_ERROR = "Proto class is misconfigured";

    private Descriptors.Descriptor protoFieldDescriptor;

    public ProtoType(String protoClassName) {
        try {
            Class<?> protoClass = Class.forName(protoClassName);
            protoFieldDescriptor = (Descriptors.Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
        } catch (ReflectiveOperationException exception) {
            throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR, exception);
        }
    }

    public String[] getFieldNames() {
        return protoFieldDescriptor.getFields().stream().map(fieldDescriptor -> fieldDescriptor.getName()).toArray(String[]::new);
    }
}
