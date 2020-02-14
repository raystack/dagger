package com.gojek.daggers.protoHandler;

import com.google.protobuf.DynamicMessage;

public interface ProtoHandler {
    boolean canHandle();

    DynamicMessage.Builder getProtoBuilder(DynamicMessage.Builder builder, Object field);

    Object getTypeAppropriateValue(Object field);
}
