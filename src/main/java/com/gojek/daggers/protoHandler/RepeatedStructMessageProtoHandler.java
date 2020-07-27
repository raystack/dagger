package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class RepeatedStructMessageProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public RepeatedStructMessageProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE &&
                fieldDescriptor.toProto().getTypeName().equals(".google.protobuf.Struct") && fieldDescriptor.isRepeated();
    }

    @Override
    public DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Object field) {
        return builder;
    }

    @Override
    public Object transformForPostProcessor(Object field) {
        return null;
    }

    @Override
    public Object transformForKafka(Object field) {
        return null;
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{}));
    }
}
