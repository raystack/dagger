package com.gojek.daggers.source;

import com.gojek.daggers.exception.DaggerProtoException;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ProtoDeserializer implements KeyedDeserializationSchema<Row> {

    private String protoClassName;
    private ProtoType protoType;
    private int timestampFieldIndex;
    private StencilClient stencilClient;

    public ProtoDeserializer(String protoClassName, int timestampFieldIndex, String rowtimeAttributeName, StencilClient stencilClient) {
        this.protoClassName = protoClassName;
        this.protoType = new ProtoType(protoClassName, rowtimeAttributeName, stencilClient);
        this.timestampFieldIndex = timestampFieldIndex;
        this.stencilClient = stencilClient;
    }

    private Descriptors.Descriptor getProtoParser() {
        Descriptors.Descriptor dsc = stencilClient.get(protoClassName);
        if (dsc == null) {
            throw new DaggerProtoException();
        }
        return dsc;
    }

    private Row getRow(DynamicMessage proto) {
        List<Descriptors.FieldDescriptor> fields = proto.getDescriptorForType().getFields();
        Row row = new Row(fields.size());
        for (Descriptors.FieldDescriptor field : fields) {
            if (field.isMapField()) {
                List<DynamicMessage> mapEntries = (List<DynamicMessage>) proto.getField(field);
                row.setField(field.getIndex(), getMapRow(mapEntries));
                continue;
            }

            if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                row.setField(field.getIndex(), proto.getField(field).toString());
            } else if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                if (field.toProto().getTypeName().equals(".google.protobuf.Struct")) {
                    continue;
                }
                if (field.isRepeated()) {
                    row.setField(field.getIndex(), getRow((List<DynamicMessage>) proto.getField(field)));
                } else {
                    row.setField(field.getIndex(), getRow((DynamicMessage) proto.getField(field)));
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

    private Object[] getRow(List<DynamicMessage> protos) {
        ArrayList<Row> rows = new ArrayList<>();
        protos.forEach(generatedMessageV3 -> rows.add(getRow(generatedMessageV3)));
        return rows.toArray();
    }

    private Object[] getMapRow(List<DynamicMessage> protos) {
        ArrayList<Row> rows = new ArrayList<>();
        protos.forEach(entry -> rows.add(getRowFromMap(entry)));
        return rows.toArray();
    }

    private Row getRowFromMap(DynamicMessage protos) {
        Row row = new Row(2);
        Object[] keyValue = protos.getAllFields().values().toArray();
        row.setField(0, keyValue.length > 0 ? keyValue[0] : "");
        row.setField(1, keyValue.length > 1 ? keyValue[1] : "");
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
            DynamicMessage proto = DynamicMessage.parseFrom(getProtoParser(), message);
            return addTimestampFieldToRow(getRow(proto), proto);
        } catch (RuntimeException e) {
            throw new DaggerProtoException(e);
        }
    }

    private Row addTimestampFieldToRow(Row row, DynamicMessage proto) {
        Descriptors.FieldDescriptor fieldDescriptor = proto.getDescriptorForType().findFieldByNumber(timestampFieldIndex);

        Row finalRecord = new Row(row.getArity() + 1);
        for (int fieldIndex = 0; fieldIndex < row.getArity(); fieldIndex++) {
            finalRecord.setField(fieldIndex, row.getField(fieldIndex));
        }
        Row timestampRow = getRow((DynamicMessage) proto.getField(fieldDescriptor));
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
