package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MapEntry;
import com.google.protobuf.WireFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
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
    public DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }

        if (field instanceof Map) convertFromMap(builder, (Map<String, String>) field);

        if (field instanceof Object[]) convertFromRow(builder, (Object[]) field);

        return builder;
    }

    @Override
    public Object transformForPostProcessor(Object field) {
        ArrayList<Row> rows = new ArrayList<>();
        if (field != null) {
            Map<String, String> mapField = (Map<String, String>) field;
            for (Entry<String, String> entry : mapField.entrySet()) {
                rows.add(getRowFromMap(entry));
            }
        }
        return rows.toArray();
    }

    @Override
    public Object transformForKafka(Object field) {
        ArrayList<Row> rows = new ArrayList<>();
        if (field != null) {
            List<DynamicMessage> protos = (List<DynamicMessage>) field;
            protos.forEach(proto -> rows.add(getRowFromMap(proto)));
        }
        return rows.toArray();
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.OBJECT_ARRAY(TypeInformationFactory.getRowType(fieldDescriptor.getMessageType()));
    }

    private Row getRowFromMap(Entry<String, String> entry) {
        Row row = new Row(2);
        row.setField(0, entry.getKey());
        row.setField(1, entry.getValue());
        return row;
    }

    private Row getRowFromMap(DynamicMessage proto) {
        Row row = new Row(2);
        Object[] keyValue = proto.getAllFields().values().toArray();
        row.setField(0, keyValue.length > 0 ? keyValue[0] : "");
        row.setField(1, keyValue.length > 1 ? keyValue[1] : "");
        return row;
    }

    private void convertFromRow(DynamicMessage.Builder builder, Object[] field) {
        for (Object inputValue : field) {
            Row inputRow = (Row) inputValue;
            if (inputRow.getArity() != 2)
                throw new IllegalArgumentException("Row: " + inputRow.toString() + " of size: " + inputRow.getArity() + " cannot be converted to map");
            MapEntry<String, String> mapEntry = MapEntry
                    .newDefaultInstance(fieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
            builder.addRepeatedField(fieldDescriptor,
                    mapEntry.toBuilder()
                            .setKey((String) inputRow.getField(0))
                            .setValue((String) inputRow.getField(1))
                            .buildPartial());
        }
    }

    private void convertFromMap(DynamicMessage.Builder builder, Map<String, String> field) {
        for (Entry<String, String> entry : field.entrySet()) {
            MapEntry<String, String> mapEntry = MapEntry.newDefaultInstance(fieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
            builder.addRepeatedField(fieldDescriptor,
                    mapEntry.toBuilder()
                            .setKey(entry.getKey())
                            .setValue(entry.getValue())
                            .buildPartial());
        }
    }
}
