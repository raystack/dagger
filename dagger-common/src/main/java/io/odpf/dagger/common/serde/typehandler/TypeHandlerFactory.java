package io.odpf.dagger.common.serde.typehandler;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.serde.typehandler.complex.EnumTypeHandler;
import io.odpf.dagger.common.serde.typehandler.complex.MapTypeHandler;
import io.odpf.dagger.common.serde.typehandler.complex.MessageTypeHandler;
import io.odpf.dagger.common.serde.typehandler.complex.StructMessageTypeHandler;
import io.odpf.dagger.common.serde.typehandler.complex.TimestampTypeHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedEnumTypeHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedMessageTypeHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedPrimitiveTypeHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedStructMessageTypeHandler;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The factory class for Proto handler.
 */
public class TypeHandlerFactory {
    private static Map<String, TypeHandler> protoHandlerMap = new ConcurrentHashMap<>();

    /**
     * Gets proto handler.
     *
     * @param fieldDescriptor the field descriptor
     * @return the proto handler
     */
    public static TypeHandler getProtoHandler(final Descriptors.FieldDescriptor fieldDescriptor) {
        return protoHandlerMap.computeIfAbsent(fieldDescriptor.getFullName(),
                k -> getSpecificHandlers(fieldDescriptor).stream().filter(TypeHandler::canHandle)
                        .findFirst().orElseGet(() -> new PrimitiveTypeHandler(fieldDescriptor)));
    }

    /**
     * Clear proto handler map.
     */
    protected static void clearProtoHandlerMap() {
        protoHandlerMap.clear();
    }

    private static List<TypeHandler> getSpecificHandlers(Descriptors.FieldDescriptor fieldDescriptor) {
        return Arrays.asList(
                new MapTypeHandler(fieldDescriptor),
                new TimestampTypeHandler(fieldDescriptor),
                new EnumTypeHandler(fieldDescriptor),
                new StructMessageTypeHandler(fieldDescriptor),
                new RepeatedStructMessageTypeHandler(fieldDescriptor),
                new RepeatedPrimitiveTypeHandler(fieldDescriptor),
                new RepeatedMessageTypeHandler(fieldDescriptor),
                new RepeatedEnumTypeHandler(fieldDescriptor),
                new MessageTypeHandler(fieldDescriptor)
        );
    }
}
