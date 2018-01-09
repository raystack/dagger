package com.gojek.daggers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

import java.lang.reflect.Method;


public class ProtoSerializer implements KeyedSerializationSchema<Row> {

  private static final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class is misconfigured";
  public static final int MILLI_TO_SECONDS = 1000;
  private String protoClassNamePrefix;
  private String[] columnNames;

  public ProtoSerializer(String protoClassNamePrefix, String[] columnNames) {
    this.protoClassNamePrefix = protoClassNamePrefix;
    this.columnNames = columnNames;
  }

  @Override
  public byte[] serializeKey(Row element) {
    return serialize(element, "Key");
  }

  @Override
  public byte[] serializeValue(Row element) {
    return serialize(element, "Message");
  }

  private byte[] serialize(Row element, String suffix) {
    DynamicMessage message;
    try {
      Class<?> protoClass = Class.forName(protoClassNamePrefix + suffix);
      Method getDescriptor = protoClass.getMethod("getDescriptor");
      message = parse(element, (Descriptors.Descriptor) getDescriptor.invoke(null));
    } catch (ReflectiveOperationException exception) {
      throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR, exception);
    }
    return message.toByteArray();
  }

  private DynamicMessage parse(Row element, Descriptors.Descriptor descriptor) {
    int numberOfElements = element.getArity();
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (int index = 0; index < numberOfElements; index++) {
      Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(columnNames[index]);
      if (fieldDescriptor == null) {
        continue;

      }
      if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp")) {
        java.sql.Timestamp timestampField = (java.sql.Timestamp) element.getField(index);
        long timestampSeconds = timestampField.getTime() / MILLI_TO_SECONDS;
        int timestampNanos = timestampField.getNanos();
        Timestamp timestamp = Timestamp
            .newBuilder()
            .setSeconds(timestampSeconds)
            .setNanos(timestampNanos)
            .build();
        builder.setField(fieldDescriptor, timestamp);
      } else if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
        builder.setField(fieldDescriptor, fieldDescriptor.getEnumType().findValueByName(String.valueOf(element.getField(index))));
      } else {
        builder.setField(fieldDescriptor, element.getField(index));
      }

    }
    return builder.build();
  }

  @Override
  public String getTargetTopic(Row element) {
    return null;
  }
}
