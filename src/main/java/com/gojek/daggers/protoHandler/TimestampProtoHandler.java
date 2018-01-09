package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;

public class TimestampProtoHandler implements ProtoHandler {
  public static final int MILLI_TO_SECONDS = 1000;
  private Descriptors.FieldDescriptor fieldDescriptor;

  public TimestampProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
    this.fieldDescriptor = fieldDescriptor;
  }

  @Override
  public boolean canPopulate() {
    return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp");
  }

  @Override
  public DynamicMessage.Builder populate(DynamicMessage.Builder builder, Object field) {
    if (!canPopulate())
      return builder;
    java.sql.Timestamp timestampField = (java.sql.Timestamp) field;
    long timestampSeconds = timestampField.getTime() / MILLI_TO_SECONDS;
    int timestampNanos = timestampField.getNanos();
    Timestamp timestamp = Timestamp
        .newBuilder()
        .setSeconds(timestampSeconds)
        .setNanos(timestampNanos)
        .build();
    builder.setField(fieldDescriptor, timestamp);
    return builder;
  }
}
