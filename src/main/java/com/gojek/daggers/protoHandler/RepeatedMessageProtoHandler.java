package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import net.minidev.json.JSONArray;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

public class RepeatedMessageProtoHandler implements ProtoHandler {
    private FieldDescriptor fieldDescriptor;

    public RepeatedMessageProtoHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == MESSAGE && fieldDescriptor.isRepeated();
    }

    @Override
    public Builder getProtoBuilder(Builder builder, Object field) {
        if (!canHandle()) {
            return builder;
        }

        Object[] rowElements = (Object[]) field;
        ArrayList<DynamicMessage> messages = new ArrayList<>();

        List<FieldDescriptor> nestedFieldDescriptors = fieldDescriptor.getMessageType().getFields();

        for (Object row : rowElements) {
            Builder elementBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
            handleNestedField(elementBuilder, nestedFieldDescriptors, (Row) row);
            messages.add(elementBuilder.build());
        }

        return builder.setField(fieldDescriptor, messages);
    }

    @Override
    public Object getTypeAppropriateValue(Object field) {
        ArrayList<Row> rows = new ArrayList<>();
        if (field != null) {
            Object[] inputFields = ((JSONArray) field).toArray();
            for (Object inputField : inputFields) {
                rows.add(RowFactory.createRow((Map<String, Object>) inputField, fieldDescriptor.getMessageType()));
            }
        }
        return rows.toArray();
    }

    private void handleNestedField(Builder elementBuilder, List<FieldDescriptor> nestedFieldDescriptors, Row row) {
        for (FieldDescriptor nestedFieldDescriptor : nestedFieldDescriptors) {
            int index = nestedFieldDescriptor.getIndex();

            if (index < row.getArity()) {
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(nestedFieldDescriptor);
                protoHandler.getProtoBuilder(elementBuilder, row.getField(index));
            }
        }
    }
}
