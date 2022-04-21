package io.odpf.dagger.common.serde.typehandler.complex;

import io.odpf.dagger.common.serde.typehandler.TypeHandler;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;

/**
 * The type Struct message proto handler.
 */
public class StructMessageTypeHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Struct message proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public StructMessageTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                && fieldDescriptor.toProto().getTypeName().equals(".google.protobuf.Struct") && !fieldDescriptor.isRepeated();
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
    public Object transformFromParquet(SimpleGroup simpleGroup) {
        return null;
    }

    @Override
    public Object transformToJson(Object field) {
        return null;
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.ROW_NAMED(new String[]{});
    }
}
