package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MapEntry;
import com.google.protobuf.WireFormat;

import java.util.Map;

public class MapProtoHandler implements ProtoHandler {

    private Descriptors.FieldDescriptor fieldDescriptor;

    public MapProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {

        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canPopulate() {
        return fieldDescriptor.isMapField();
    }

    @Override
    public DynamicMessage.Builder populate(DynamicMessage.Builder builder, Object field) {
        if (!canPopulate()) {
            return builder;
        }

        Map<String, String> mapField = (Map<String, String>) field;
        for (Map.Entry<String, String> entry : mapField.entrySet()) {
            MapEntry<String, String> mapEntry = MapEntry.newDefaultInstance(fieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
            builder.addRepeatedField(fieldDescriptor, mapEntry.toBuilder().setKey(entry.getKey()).setValue(entry.getValue()).buildPartial());
        }

        return builder;
    }
}
