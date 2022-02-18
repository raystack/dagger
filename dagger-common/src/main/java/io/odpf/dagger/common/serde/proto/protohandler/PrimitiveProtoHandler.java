package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import io.odpf.dagger.common.exceptions.serde.InvalidDataTypeException;
import io.odpf.dagger.common.serde.proto.protohandler.typehandler.PrimitiveTypeHandlerFactory;
import io.odpf.dagger.common.serde.proto.protohandler.typehandler.PrimitiveTypeHandler;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

/**
 * The type Primitive proto handler.
 */
public class PrimitiveProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Primitive proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public PrimitiveProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return true;
    }

    @Override
    public DynamicMessage.Builder transformForKafka(DynamicMessage.Builder builder, Object field) {
        return field != null ? builder.setField(fieldDescriptor, transform(field)) : builder;
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        return transform(field);
    }

    private Object transform(Object field) {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor);
        try {
            return primitiveTypeHandler.getValue(field);
        } catch (NumberFormatException e) {
            String errMessage = String.format("type mismatch of field: %s, expecting %s type, actual type %s", fieldDescriptor.getName(), fieldDescriptor.getType(), field.getClass());
            throw new InvalidDataTypeException(errMessage);
        }
    }

    @Override
    public Object transformFromKafka(Object field) {
        return field;
    }

    @Override
    public Object transformToJson(Object field) {
        return field;
    }

    @Override
    public TypeInformation getTypeInformation() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor);
        return primitiveTypeHandler.getTypeInformation();
    }

}
