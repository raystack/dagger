package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import io.odpf.dagger.common.exceptions.serde.DataTypeNotSupportedException;
import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The factory class for Primitive type handler.
 */
public class PrimitiveTypeHandlerFactory {
    /**
     * Gets type handler.
     *
     * @param fieldDescriptor the field descriptor
     * @return the type handler
     */
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
