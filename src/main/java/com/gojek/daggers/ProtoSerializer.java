package com.gojek.daggers;

import com.gojek.daggers.protoHandler.ProtoHandler;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

import java.lang.reflect.Method;


public class ProtoSerializer implements KeyedSerializationSchema<Row> {

  private static final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class is misconfigured";
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
      ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
      builder = protoHandler.populate(builder, element.getField(index));
    }
    return builder.build();
  }

  @Override
  public String getTargetTopic(Row element) {
    return null;
  }
}
