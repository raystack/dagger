package com.gotocompany.dagger.common.serde.typehandler.repeated;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandler;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;

/**
 * The type Repeated struct message proto handler.
 */
public class RepeatedStructMessageHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Repeated struct message proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public RepeatedStructMessageHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                && fieldDescriptor.toProto().getTypeName().equals(".google.protobuf.Struct") && fieldDescriptor.isRepeated();
    }

    @Override
    public DynamicMessage.Builder transformToProtoBuilder(DynamicMessage.Builder builder, Object field) {
        return builder;
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        return null;
    }

    @Override
    public Object transformFromProto(Object field) {
        return null;
    }

    @Override
    public Object transformFromProtoUsingCache(Object field, FieldDescriptorCache cache) {
        return null;
    }

    @Override
    public Object transformFromParquet(SimpleGroup simpleGroup) {
        return null;
    }

    @Override
    public Object transformToJson(Object field) {
        return null;
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{}));
    }
}
