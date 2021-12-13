package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;

/**
 * The type Repeated enum proto handler.
 */
public class RepeatedEnumProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private static final Gson GSON = new Gson();

    /**
     * Instantiates a new Repeated enum proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public RepeatedEnumProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == ENUM && fieldDescriptor.isRepeated();
    }

    @Override
    public DynamicMessage.Builder transformForKafka(DynamicMessage.Builder builder, Object field) {
        return builder;
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        return getValue(field);
    }

    @Override
    public Object transformFromKafka(Object field) {
        return getValue(field);
    }

    @Override
    public Object transformToJson(Object field) {
        return GSON.toJson(getValue(field));
    }

    @Override
    public TypeInformation getTypeInformation() {
        return ObjectArrayTypeInfo.getInfoFor(Types.STRING);
    }

    private Object getValue(Object field) {
        List<String> values = new ArrayList<>();
        if (field != null) {
            values = getStringRow((List) field);
        }
        return values.toArray(new String[]{});
    }

    private List<String> getStringRow(List<Object> protos) {
        return protos
                .stream()
                .map(String::valueOf)
                .collect(Collectors.toList());
    }
}
