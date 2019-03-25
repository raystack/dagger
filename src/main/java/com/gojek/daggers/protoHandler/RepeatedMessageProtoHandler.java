package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

public class RepeatedMessageProtoHandler implements ProtoHandler {
    private FieldDescriptor fieldDescriptor;

    public RepeatedMessageProtoHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canPopulate() {
        return fieldDescriptor.getJavaType() == MESSAGE && fieldDescriptor.isRepeated();
    }

    @Override
    public Builder populate(Builder builder, Object field) {
        if (!canPopulate()) {
            return builder;
        }

        Row[] rowElements = (Row[]) field;
        ArrayList<DynamicMessage> messages = new ArrayList<>();

        List<FieldDescriptor> nestedFieldDescriptors = fieldDescriptor.getMessageType().getFields();
        Builder elementBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());

        for (Row row : rowElements) {
            handleNestedField(elementBuilder, nestedFieldDescriptors, row);
            messages.add(elementBuilder.build());
        }

        return builder.setField(fieldDescriptor, messages);
    }

    private void handleNestedField(Builder elementBuilder, List<FieldDescriptor> nestedFieldDescriptors, Row row) {
        for (FieldDescriptor nestedFieldDescriptor : nestedFieldDescriptors) {
            int index = nestedFieldDescriptor.getIndex();

            if (index < row.getArity()) {
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(nestedFieldDescriptor);
                protoHandler.populate(elementBuilder, row.getField(index));
            }
        }
    }
}
