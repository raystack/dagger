package com.gojek.daggers.protoHandler;

import com.google.protobuf.DynamicMessage;

public interface ProtoHandler {
    boolean canPopulate();

    DynamicMessage.Builder populate(DynamicMessage.Builder builder, Object field);
}
