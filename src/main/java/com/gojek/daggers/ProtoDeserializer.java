package com.gojek.daggers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;


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
            if (field.isMapField() || field.isRepeated()) {
                continue;
            }

            if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                row.setField(field.getIndex(), proto.getField(field).toString());
            } else if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                row.setField(field.getIndex(), getRow((GeneratedMessageV3) proto.getField(field)));
            } else {
                row.setField(field.getIndex(), proto.getField(field));
            }
        }
        return row;
    }

    @Override
    public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        try {
            GeneratedMessageV3 proto = (GeneratedMessageV3) getProtoParser().invoke(null, message);
            return getRow(proto);
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
