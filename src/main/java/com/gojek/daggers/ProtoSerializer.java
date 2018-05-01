package com.gojek.daggers;

import com.gojek.daggers.protoHandler.ProtoHandler;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

public class ProtoSerializer implements KeyedSerializationSchema<Row> {

  private String protoClassNamePrefix;
  private String[] columnNames;

  public ProtoSerializer(String protoClassNamePrefix, String[] columnNames) {
    this.protoClassNamePrefix = protoClassNamePrefix;
    this.columnNames = columnNames;
  }

  private Descriptors.Descriptor getDescriptor(String className) {
    return DescriptorStore.get(className);
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
    DynamicMessage message = parse(element, getDescriptor(protoClassNamePrefix + suffix));
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
