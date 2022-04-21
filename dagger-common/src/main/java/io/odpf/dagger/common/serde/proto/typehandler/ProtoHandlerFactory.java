package io.odpf.dagger.common.serde.proto.typehandler;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.serde.proto.typehandler.complex.EnumProtoHandler;
import io.odpf.dagger.common.serde.proto.typehandler.complex.MapProtoHandler;
import io.odpf.dagger.common.serde.proto.typehandler.complex.MessageProtoHandler;
import io.odpf.dagger.common.serde.proto.typehandler.complex.StructMessageProtoHandler;
import io.odpf.dagger.common.serde.proto.typehandler.complex.TimestampProtoHandler;
import io.odpf.dagger.common.serde.proto.typehandler.repeated.RepeatedEnumProtoHandler;
import io.odpf.dagger.common.serde.proto.typehandler.repeated.RepeatedMessageProtoHandler;
import io.odpf.dagger.common.serde.proto.typehandler.repeated.RepeatedPrimitiveProtoHandler;
import io.odpf.dagger.common.serde.proto.typehandler.repeated.RepeatedStructMessageProtoHandler;

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
