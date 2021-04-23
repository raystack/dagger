package io.odpf.dagger.core.protohandler;

import com.google.protobuf.Descriptors;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ProtoHandlerFactory {
    private static final Map<String, ProtoHandler> protoHandlerMap = new ConcurrentHashMap<>();

    public static ProtoHandler getProtoHandler(final Descriptors.FieldDescriptor fieldDescriptor) {
        return protoHandlerMap.computeIfAbsent(fieldDescriptor.getFullName(),
                k -> getSpecificHandlers(fieldDescriptor).stream().filter(ProtoHandler::canHandle)
                        .findFirst().orElseGet(() -> new PrimitiveProtoHandler(fieldDescriptor)));
    }

    protected static void clearProtoHandlerMap() {
        protoHandlerMap.clear();
    }

    private static List<ProtoHandler> getSpecificHandlers(Descriptors.FieldDescriptor fieldDescriptor) {
        return Arrays.asList(
                new MapProtoHandler(fieldDescriptor),
                new TimestampProtoHandler(fieldDescriptor),
                new EnumProtoHandler(fieldDescriptor),
                new StructMessageProtoHandler(fieldDescriptor),
                new RepeatedStructMessageProtoHandler(fieldDescriptor),
                new RepeatedPrimitiveProtoHandler(fieldDescriptor),
                new RepeatedMessageProtoHandler(fieldDescriptor),
                new RepeatedEnumProtoHandler(fieldDescriptor),
                new MessageProtoHandler(fieldDescriptor)
        );
    }
}
