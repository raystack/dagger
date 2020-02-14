package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.daggers.exception.DataTypeNotSupportedException;
import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class TypeHandlerFactory {
    public static TypeHandler getTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        Optional<TypeHandler> filteredTypeHandlers =
                getSpecificHandlers(fieldDescriptor)
                        .stream()
                        .filter(TypeHandler::canHandle)
                        .findFirst();
        return filteredTypeHandlers.orElseThrow(() -> new DataTypeNotSupportedException("Data type " + fieldDescriptor.getJavaType() + " not supported"));
    }

    private static List<TypeHandler> getSpecificHandlers(Descriptors.FieldDescriptor fieldDescriptor) {
        return Arrays.asList(
                new IntegerTypeHandler(fieldDescriptor),
                new BooleanTypeHandler(fieldDescriptor),
                new ByteStringTypeHandler(fieldDescriptor),
                new DoubleTypeHandler(fieldDescriptor),
                new FloatTypeHandler(fieldDescriptor),
                new LongTypeHandler(fieldDescriptor),
                new StringTypeHandler(fieldDescriptor)
        );
    }
}
