package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

public class MessageProtoHandler implements ProtoHandler {
    private FieldDescriptor fieldDescriptor;

    public MessageProtoHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == MESSAGE && !fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp");
    }

    @Override
    public Builder getProtoBuilder(Builder builder, Object field) {
        if (!canHandle()) {
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
                    protoHandler.getProtoBuilder(elementBuilder, rowElement.getField(index));
                }
            }
        }

        return builder.setField(fieldDescriptor, elementBuilder.build());
    }

    @Override
    public Object getTypeAppropriateValue(Object field) {
        return RowFactory.createRow((Map<String, Object>) field, fieldDescriptor.getMessageType());
    }
}
