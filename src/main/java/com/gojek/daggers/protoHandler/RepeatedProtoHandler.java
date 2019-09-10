package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage.Builder;

import org.apache.flink.types.Row;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

import java.util.ArrayList;

/**
 * RepeatedProtoHandler
 */
public class RepeatedProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public RepeatedProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canPopulate() {
        return this.fieldDescriptor.isRepeated() && fieldDescriptor.getJavaType() != MESSAGE;
    }

    @Override
    public Builder populate(Builder builder, Object field) {
        if (!canPopulate()) {
            return builder;
        }

        Row[] elements = (Row[]) field;
        ArrayList messages = new ArrayList<>();

        for (Row row : elements) {
            messages.add(row.getField(0));
        }

        return builder.setField(fieldDescriptor, messages);
    }

}