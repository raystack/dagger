package com.gojek.daggers.protoHandler;

import com.google.protobuf.DynamicMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public interface ProtoHandler {
    boolean canHandle();

    DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Object field);

    Object transformForPostProcessor(Object field);

    Object transformForKafka(Object field);

    TypeInformation getTypeInformation();
}
