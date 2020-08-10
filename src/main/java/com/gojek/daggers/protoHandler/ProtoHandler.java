package com.gojek.daggers.protoHandler;

import com.google.protobuf.DynamicMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public interface ProtoHandler {
    boolean canHandle();

    DynamicMessage.Builder transformForKafka(DynamicMessage.Builder builder, Object field);

    Object transformFromPostProcessor(Object field);

    Object transformFromKafka(Object field);

    TypeInformation getTypeInformation();
}
