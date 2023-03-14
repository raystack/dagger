package com.gotocompany.dagger.common.serde.typehandler;

import com.google.protobuf.Descriptors;
import com.gotocompany.dagger.common.serde.typehandler.repeated.RepeatedEnumHandler;
import com.gotocompany.dagger.common.serde.typehandler.repeated.RepeatedMessageHandler;
import com.gotocompany.dagger.common.serde.typehandler.repeated.RepeatedPrimitiveHandler;
import com.gotocompany.dagger.common.serde.typehandler.repeated.RepeatedStructMessageHandler;
import com.gotocompany.dagger.common.serde.typehandler.complex.EnumHandler;
import com.gotocompany.dagger.common.serde.typehandler.complex.MapHandler;
import com.gotocompany.dagger.common.serde.typehandler.complex.MessageHandler;
import com.gotocompany.dagger.common.serde.typehandler.complex.StructMessageHandler;
import com.gotocompany.dagger.common.serde.typehandler.complex.TimestampHandler;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The factory class for Type handler.
 */
public class TypeHandlerFactory {
    private static Map<String, TypeHandler> typeHandlerMap = new ConcurrentHashMap<>();

    /**
     * Gets type handler.
     *
     * @param fieldDescriptor the field descriptor
     * @return the type handler
     */
    public static TypeHandler getTypeHandler(final Descriptors.FieldDescriptor fieldDescriptor) {
        return typeHandlerMap.computeIfAbsent(fieldDescriptor.getFullName(),
                k -> getSpecificHandlers(fieldDescriptor).stream().filter(TypeHandler::canHandle)
                        .findFirst().orElseGet(() -> new PrimitiveTypeHandler(fieldDescriptor)));
    }

    /**
     * Clear type handler map.
     */
    protected static void clearTypeHandlerMap() {
        typeHandlerMap.clear();
    }

    private static List<TypeHandler> getSpecificHandlers(Descriptors.FieldDescriptor fieldDescriptor) {
        return Arrays.asList(
                new MapHandler(fieldDescriptor),
                new TimestampHandler(fieldDescriptor),
                new EnumHandler(fieldDescriptor),
                new StructMessageHandler(fieldDescriptor),
                new RepeatedStructMessageHandler(fieldDescriptor),
                new RepeatedPrimitiveHandler(fieldDescriptor),
                new RepeatedMessageHandler(fieldDescriptor),
                new RepeatedEnumHandler(fieldDescriptor),
                new MessageHandler(fieldDescriptor)
        );
    }
}
