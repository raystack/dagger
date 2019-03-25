package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ProtoHandlerFactory {
    public static ProtoHandler getProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        Optional<ProtoHandler> filteredProtoHandlers =
                getSpecificHandlers(fieldDescriptor)
                        .stream()
                        .filter(ProtoHandler::canPopulate)
                        .findFirst();
        return filteredProtoHandlers.orElseGet(() -> new DefaultProtoHandler(fieldDescriptor));
    }

    private static List<ProtoHandler> getSpecificHandlers(Descriptors.FieldDescriptor fieldDescriptor) {
        return Arrays.asList(
                new MapProtoHandler(fieldDescriptor),
                new TimestampProtoHandler(fieldDescriptor),
                new EnumProtoHandler(fieldDescriptor),
                new RepeatedMessageProtoHandler(fieldDescriptor),
                new MessageProtoHandler(fieldDescriptor)
        );
    }
}
