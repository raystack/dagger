package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;

import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

/**
 * The type Message proto handler.
 */
public class MessageProtoHandler implements ProtoHandler {
    private FieldDescriptor fieldDescriptor;
    private JsonRowSerializationSchema jsonRowSerializationSchema;

    /**
     * Instantiates a new Message proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public MessageProtoHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == MESSAGE && !fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp");
    }

    @Override
    public Builder transformForKafka(Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }

        Builder elementBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        List<FieldDescriptor> nestedFieldDescriptors = fieldDescriptor.getMessageType().getFields();
        Row rowElement = (Row) field;

        for (FieldDescriptor nestedFieldDescriptor : nestedFieldDescriptors) {
            int index = nestedFieldDescriptor.getIndex();
            if (index < rowElement.getArity()) {
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(nestedFieldDescriptor);
                if (rowElement.getField(index) != null) {
                    protoHandler.transformForKafka(elementBuilder, rowElement.getField(index));
                }
            }
        }

        return builder.setField(fieldDescriptor, elementBuilder.build());
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        return RowFactory.createRow((Map<String, Object>) field, fieldDescriptor.getMessageType());
    }

    @Override
    public Object transformFromKafka(Object field) {
        return RowFactory.createRow((DynamicMessage) field);
    }

    @Override
    public Object transformToJson(Object field) {
        if (jsonRowSerializationSchema == null) {
            jsonRowSerializationSchema = createJsonRowSchema();
        }
        return new String(jsonRowSerializationSchema.serialize((Row) field));
    }

    @Override
    public TypeInformation getTypeInformation() {
        return TypeInformationFactory.getRowType(fieldDescriptor.getMessageType());
    }

    private JsonRowSerializationSchema createJsonRowSchema() {
        return JsonRowSerializationSchema
                .builder()
                .withTypeInfo(getTypeInformation())
                .build();
    }
}
