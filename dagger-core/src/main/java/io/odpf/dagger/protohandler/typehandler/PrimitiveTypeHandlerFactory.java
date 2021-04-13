package io.odpf.dagger.protohandler.typehandler;

import io.odpf.dagger.exception.DataTypeNotSupportedException;
import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class PrimitiveTypeHandlerFactory {
    public static PrimitiveTypeHandler getTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        Optional<PrimitiveTypeHandler> filteredTypeHandlers =
                getSpecificHandlers(fieldDescriptor)
                        .stream()
                        .filter(PrimitiveTypeHandler::canHandle)
                        .findFirst();
        return filteredTypeHandlers.orElseThrow(() -> new DataTypeNotSupportedException("Data type " + fieldDescriptor.getJavaType() + " not supported in primitive type handlers"));
    }

    private static List<PrimitiveTypeHandler> getSpecificHandlers(Descriptors.FieldDescriptor fieldDescriptor) {
        return Arrays.asList(
                new IntegerPrimitiveTypeHandler(fieldDescriptor),
                new BooleanPrimitiveTypeHandler(fieldDescriptor),
                new DoublePrimitiveTypeHandler(fieldDescriptor),
                new FloatPrimitiveTypeHandler(fieldDescriptor),
                new LongPrimitiveTypeHandler(fieldDescriptor),
                new StringPrimitiveTypeHandler(fieldDescriptor),
                new ByteStringPrimitiveTypeHandler(fieldDescriptor)
        );
    }
}
