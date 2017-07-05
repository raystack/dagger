package com.gojek.daggers;

import com.gojek.esb.booking.BookingLogKey;
import com.gojek.esb.participant.DriverLocation;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Timestamp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;


public class ProtoDeserializer implements DeserializationSchema<Row>{

    private ProtoType protoType;
    private Method protoParser;
    private final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class is misconfigured";

    public ProtoDeserializer(String protoClassName, ProtoType protoType) {
        this.protoType = protoType;
        try {
            Class<?> protoClass = Class.forName(protoClassName);
            protoParser = protoClass.getMethod("parseFrom", byte[].class);
        } catch (ReflectiveOperationException exception) {
            throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR, exception);
        }
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            GeneratedMessageV3 proto = (GeneratedMessageV3)protoParser.invoke(null, message);
            List<Descriptors.FieldDescriptor> fields = proto.getDescriptorForType().getFields();
            Row row = new Row(fields.size());
            for (Descriptors.FieldDescriptor field: fields) {
                row.setField(field.getIndex(), proto.getField(field));
            }
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
        return Types.ROW(protoType.getFieldNames(), protoType.getFieldTypes());
    }
}
