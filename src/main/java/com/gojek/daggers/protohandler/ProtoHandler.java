package com.gojek.daggers.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.protobuf.DynamicMessage;

public interface ProtoHandler {
    boolean canHandle();

    DynamicMessage.Builder transformForKafka(DynamicMessage.Builder builder, Object field);

    Object transformFromPostProcessor(Object field);

    Object transformFromKafka(Object field);

    Object transformToJson(Object field);

    TypeInformation getTypeInformation();
}
