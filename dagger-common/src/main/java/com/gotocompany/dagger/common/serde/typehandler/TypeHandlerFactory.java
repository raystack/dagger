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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The factory class for Type handler.
 */
public class TypeHandlerFactory {
    private static Map<String, Pair<Integer, TypeHandler>> typeHandlerMap = new ConcurrentHashMap<>();

    /**
     * Gets type handler.
     *
     * @param fieldDescriptor the field descriptor
     * @return the type handler
     */
    public static TypeHandler getTypeHandler(final Descriptors.FieldDescriptor fieldDescriptor) {
        int newHashCode = fieldDescriptor.hashCode();

        /* this means we have already created and persisted the handler corresponding to
         the field descriptor in the map and hence we can directly return it */
        if (typeHandlerMap.containsKey(fieldDescriptor.getFullName()) && typeHandlerMap.get(fieldDescriptor.getFullName()).getKey() == newHashCode) {
            Pair<Integer, TypeHandler> pair = typeHandlerMap.get(fieldDescriptor.getFullName());
            return pair.getValue();
        } else {
            /* this means that either it is a new field not encountered before and/or same field but with an updated field descriptor object
            in either case, we create a new handler and persist it in the map */
            TypeHandler handler = getSpecificHandlers(fieldDescriptor)
                    .stream()
                    .filter(TypeHandler::canHandle)
                    .findFirst()
                    .orElseGet(() -> new PrimitiveTypeHandler(fieldDescriptor));
            Pair<Integer, TypeHandler> pair = ImmutablePair.of(newHashCode, handler);
            typeHandlerMap.put(fieldDescriptor.getFullName(), pair);
            return handler;
        }
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
