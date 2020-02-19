package com.gojek.daggers.protoHandler;

import com.google.protobuf.DynamicMessage;

public interface ProtoHandler {
    boolean canHandle();

    DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Object field);

    Object transform(Object field);
}
