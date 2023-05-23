package com.gotocompany.dagger.common.serde.typehandler.repeated;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.serde.typehandler.primitive.PrimitiveHandler;
import com.gotocompany.dagger.common.serde.typehandler.primitive.PrimitiveHandlerFactory;
import com.gotocompany.dagger.common.serde.typehandler.PrimitiveTypeHandler;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandler;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.gson.Gson;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

/**
 * The type Repeated primitive proto handler.
 */
public class RepeatedPrimitiveHandler implements TypeHandler {
    private final FieldDescriptor fieldDescriptor;
    private static final Gson GSON = new Gson();

    /**
     * Instantiates a new Repeated primitive proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public RepeatedPrimitiveHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.isRepeated() && fieldDescriptor.getJavaType() != MESSAGE && fieldDescriptor.getJavaType() != ENUM;
    }

    @Override
    public DynamicMessage.Builder transformToProtoBuilder(DynamicMessage.Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }
        if (field.getClass().isArray()) {
            field = Arrays.asList((Object[]) field);
        }
        return builder.setField(fieldDescriptor, field);
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        ArrayList<Object> outputValues = new ArrayList<>();
        if (field != null) {
            List<Object> inputValues = (List<Object>) field;
            PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(fieldDescriptor);
            for (Object inputField : inputValues) {
                outputValues.add(primitiveTypeHandler.transformFromPostProcessor(inputField));
            }
        }
        return outputValues;
    }

    @Override
    public Object transformFromProto(Object field) {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory.getTypeHandler(fieldDescriptor);
        return primitiveHandler.parseRepeatedObjectField(field);
    }

    @Override
    public Object transformFromProtoUsingCache(Object field, FieldDescriptorCache cache) {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory.getTypeHandler(fieldDescriptor);
        return primitiveHandler.parseRepeatedObjectField(field);
    }

    @Override
    public Object transformFromParquet(SimpleGroup simpleGroup) {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory.getTypeHandler(fieldDescriptor);
        return primitiveHandler.parseRepeatedSimpleGroupField(simpleGroup);
    }

    @Override
    public Object transformToJson(Object field) {
        return GSON.toJson(field);
    }

    @Override
    public TypeInformation getTypeInformation() {
        return PrimitiveHandlerFactory.getTypeHandler(fieldDescriptor).getArrayType();
    }
}
