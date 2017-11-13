package com.gojek.daggers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.UnmodifiableLazyStringList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.MapEntry;


public class ProtoDeserializer implements KeyedDeserializationSchema<Row> {

    private String protoClassName;
    private ProtoType protoType;
    private transient Method protoParser;
    private static final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class is misconfigured";

    public ProtoDeserializer(String protoClassName, ProtoType protoType) {
        this.protoClassName = protoClassName;
        this.protoType = protoType;
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
                        row.setField(field.getIndex(), getRow((UnmodifiableLazyStringList) proto.getField(field)));
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

    private String[] getRow(UnmodifiableLazyStringList listField) {
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
            Row row = getRow(proto);
            return row;
        } catch (ReflectiveOperationException e) {
            throw new ProtoDeserializationExcpetion(e);
        }
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
