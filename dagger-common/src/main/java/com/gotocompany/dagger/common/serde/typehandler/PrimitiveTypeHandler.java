package com.gotocompany.dagger.common.serde.typehandler;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.exceptions.serde.InvalidDataTypeException;
import com.gotocompany.dagger.common.serde.typehandler.primitive.PrimitiveHandler;
import com.gotocompany.dagger.common.serde.typehandler.primitive.PrimitiveHandlerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;

/**
 * The type Primitive proto handler.
 */
public class PrimitiveTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Primitive proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public PrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return true;
    }

    @Override
    public DynamicMessage.Builder transformToProtoBuilder(DynamicMessage.Builder builder, Object field) {
        return field != null ? builder.setField(fieldDescriptor, transform(field)) : builder;
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        return transform(field);
    }

    private Object transform(Object field) {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory.getTypeHandler(fieldDescriptor);
        try {
            return primitiveHandler.parseObject(field);
        } catch (NumberFormatException e) {
            String errMessage = String.format("type mismatch of field: %s, expecting %s type, actual type %s", fieldDescriptor.getName(), fieldDescriptor.getType(), field.getClass());
            throw new InvalidDataTypeException(errMessage);
        }
    }

    @Override
    public Object transformFromProto(Object field) {
        return field;
    }

    @Override
    public Object transformFromProtoUsingCache(Object field, FieldDescriptorCache cache) {
        return field;
    }

    @Override
    public Object transformFromParquet(SimpleGroup simpleGroup) {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory.getTypeHandler(fieldDescriptor);
        return primitiveHandler.parseSimpleGroup(simpleGroup);
    }

    @Override
    public Object transformToJson(Object field) {
        return field;
    }

    @Override
    public TypeInformation getTypeInformation() {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory.getTypeHandler(fieldDescriptor);
        return primitiveHandler.getTypeInformation();
    }

}
