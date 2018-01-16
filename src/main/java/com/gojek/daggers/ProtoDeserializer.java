package com.gojek.daggers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MapEntry;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


public class ProtoDeserializer implements KeyedDeserializationSchema<Row> {

    private static final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class is misconfigured";
    private String protoClassName;
    private ProtoType protoType;
    private int timestampFieldIndex;
    private transient Method protoParser;

    public ProtoDeserializer(String protoClassName, int timestampFieldIndex, String rowtimeAttributeName) {
        this.protoClassName = protoClassName;
        this.protoType = new ProtoType(protoClassName, rowtimeAttributeName);
        this.timestampFieldIndex = timestampFieldIndex;
    }

    private Method createProtoParser() {
        try {
            Class<?> protoClass = Class.forName(protoClassName);
            return protoClass.getMethod("parseFrom", byte[].class);
        } catch (ReflectiveOperationException exception) {
            throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR, exception);
        }
    }

    private Method getProtoParser() {
        if (protoParser == null) {
            protoParser = createProtoParser();
        }
        return protoParser;
    }

    private Row getRow(GeneratedMessageV3 proto) {
        List<Descriptors.FieldDescriptor> fields = proto.getDescriptorForType().getFields();
        Row row = new Row(fields.size());
        for (Descriptors.FieldDescriptor field : fields) {
            if (field.isMapField()) {
                List<MapEntry<Object, Object>> mapEntries = (List<MapEntry<Object, Object>>) proto.getField(field);
                row.setField(field.getIndex(), getMapRow(mapEntries));
                continue;
            }

            if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                row.setField(field.getIndex(), proto.getField(field).toString());
            } else if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                if (field.isRepeated()) {
                    row.setField(field.getIndex(), getRow((List<GeneratedMessageV3>) proto.getField(field)));
                } else {
                    row.setField(field.getIndex(), getRow((GeneratedMessageV3) proto.getField(field)));
                }
            } else {
                if (field.isRepeated()) {
                    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
                        row.setField(field.getIndex(), getRowForStringList((List<String>) proto.getField(field)));
                    }
                } else {
                    row.setField(field.getIndex(), proto.getField(field));
                }
            }
        }
        return row;
    }

    private Object[] getRow(List<GeneratedMessageV3> protos) {
        ArrayList<Row> rows = new ArrayList<>();
        protos.forEach(generatedMessageV3 -> rows.add(getRow(generatedMessageV3)));
        return rows.toArray();
    }

    private Object[] getMapRow(List<MapEntry<Object, Object>> protos) {
        ArrayList<Row> rows = new ArrayList<>();
        protos.forEach(entry -> rows.add(getRow(entry)));
        return rows.toArray();
    }

    private Row getRow(MapEntry<Object, Object> protos) {
        Row row = new Row(2);
        row.setField(0, protos.getKey());
        row.setField(1, protos.getValue());
        return row;
    }

    private String[] getRowForStringList(List<String> listField) {
        String[] list = new String[listField.size()];
        for (int listIndex = 0; listIndex < listField.size(); listIndex++) {
            list[listIndex] = listField.get(listIndex);
        }
        return list;
    }


    @Override
    public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        try {
            GeneratedMessageV3 proto = (GeneratedMessageV3) getProtoParser().invoke(null, message);
            return addTimestampFieldToRow(getRow(proto), proto);
        } catch (ReflectiveOperationException e) {
            throw new ProtoDeserializationExcpetion(e);
        }
    }

    private Row addTimestampFieldToRow(Row row, GeneratedMessageV3 proto) {
        Descriptors.FieldDescriptor fieldDescriptor = proto.getDescriptorForType().getFields().get(timestampFieldIndex);

        Row finalRecord = new Row(row.getArity() + 1);
        for (int fieldIndex = 0; fieldIndex < row.getArity(); fieldIndex++) {
            finalRecord.setField(fieldIndex, row.getField(fieldIndex));
        }
        Row timestampRow = getRow((GeneratedMessageV3) proto.getField(fieldDescriptor));
        long timestampSeconds = (long) timestampRow.getField(0);
        long timestampNanos = (int) timestampRow.getField(1);

        finalRecord.setField(finalRecord.getArity() - 1, Timestamp.from(Instant.ofEpochSecond(timestampSeconds, timestampNanos)));
        return finalRecord;
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return protoType.getRowType();
    }
}
