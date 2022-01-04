package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

/**
 * The type Repeated struct message proto handler.
 */
public class RepeatedStructMessageProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Repeated struct message proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public RepeatedStructMessageProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                && fieldDescriptor.toProto().getTypeName().equals(".google.protobuf.Struct") && fieldDescriptor.isRepeated();
    }

    @Override
    public DynamicMessage.Builder transformForKafka(DynamicMessage.Builder builder, Object field) {
        return builder;
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        return null;
    }

    @Override
    public Object transformFromKafka(Object field) {
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
