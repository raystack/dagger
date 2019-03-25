package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MapEntry;
import com.google.protobuf.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MapProtoHandler implements ProtoHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapProtoHandler.class.getName());
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
