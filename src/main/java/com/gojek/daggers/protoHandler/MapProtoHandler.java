package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MapEntry;
import com.google.protobuf.WireFormat;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

public class MapProtoHandler implements ProtoHandler {

    private Descriptors.FieldDescriptor fieldDescriptor;

    public MapProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {

        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.isMapField();
    }

    @Override
    public DynamicMessage.Builder getProtoBuilder(DynamicMessage.Builder builder, Object field) {
        if (!canHandle()) {
            return builder;
        }

        Map<String, String> mapField = (Map<String, String>) field;
        for (Entry<String, String> entry : mapField.entrySet()) {
            MapEntry<String, String> mapEntry = MapEntry.newDefaultInstance(fieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
            builder.addRepeatedField(fieldDescriptor, mapEntry.toBuilder().setKey(entry.getKey()).setValue(entry.getValue()).buildPartial());
        }

        return builder;
    }

    @Override
    public Object getTypeAppropriateValue(Object field) {
        Map<String, String> mapField = (Map<String, String>) field;
        ArrayList<Row> rows = new ArrayList<>();
        for (Entry<String, String> entry : mapField.entrySet()) {
            rows.add(getRowFromMap(entry));
        }
        return rows.toArray();
    }

    private Row getRowFromMap(Entry<String, String> entry) {
        Row row = new Row(2);
        row.setField(0, entry.getKey().isEmpty() ? "" : entry.getKey());
        row.setField(1, entry.getValue().isEmpty() ? "" : entry.getValue());
        return row;
    }
}
