package io.odpf.dagger.common.serde.proto.protohandler;

import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The factory class for Proto handler.
 */
public class ProtoHandlerFactory {
    private static Map<String, ProtoHandler> protoHandlerMap = new ConcurrentHashMap<>();

    /**
     * Gets proto handler.
     *
     * @param fieldDescriptor the field descriptor
     * @return the proto handler
     */
    public static ProtoHandler getProtoHandler(final Descriptors.FieldDescriptor fieldDescriptor) {
        return protoHandlerMap.computeIfAbsent(fieldDescriptor.getFullName(),
                k -> getSpecificHandlers(fieldDescriptor).stream().filter(ProtoHandler::canHandle)
                        .findFirst().orElseGet(() -> new PrimitiveProtoHandler(fieldDescriptor)));
    }

    /**
     * Clear proto handler map.
     */
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
