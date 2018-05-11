package com.gojek.daggers;

import com.gojek.daggers.protoHandler.ProtoHandler;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.gojek.de.stencil.DescriptorStore;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

import java.util.Map;

public class ProtoSerializer implements KeyedSerializationSchema<Row> {

  private String protoClassNamePrefix;
  private String[] columnNames;
  private Map<String, String> configs;

  public ProtoSerializer(String protoClassNamePrefix, String[] columnNames, Map<String, String> configs) {
    this.protoClassNamePrefix = protoClassNamePrefix;
    this.columnNames = columnNames;
    this.configs = configs;
  }

  private Descriptors.Descriptor getDescriptor(String className) {
      DescriptorStore.loadClientIfNull(configs);
      Descriptors.Descriptor dsc = DescriptorStore.get(className);
      if (dsc == null) {
          throw new DaggerProtoException();
      }
      return dsc;
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
