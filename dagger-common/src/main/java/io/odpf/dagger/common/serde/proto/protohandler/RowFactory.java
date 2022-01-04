package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;

import java.util.List;
import java.util.Map;

/**
 * The Factory class for Row.
 */
public class RowFactory {
    /**
     * Create row from specified input map and descriptor.
     *
     * @param inputMap   the input map
     * @param descriptor the descriptor
     * @return the row
     */
    public static Row createRow(Map<String, Object> inputMap, Descriptors.Descriptor descriptor) {
        List<FieldDescriptor> descriptorFields = descriptor.getFields();
        Row row = new Row(descriptorFields.size());
        if (inputMap == null) {
            return row;
        }
        for (FieldDescriptor fieldDescriptor : descriptorFields) {
            ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
            if (inputMap.get(fieldDescriptor.getName()) != null) {
                row.setField(fieldDescriptor.getIndex(), protoHandler.transformFromPostProcessor(inputMap.get(fieldDescriptor.getName())));
            }
        }
        return row;
    }

    /**
     * Create row from specified proto and extra columns.
     *
     * @param proto        the proto
     * @param extraColumns the extra columns
     * @return the row
     */
    public static Row createRow(DynamicMessage proto, int extraColumns) {
        List<FieldDescriptor> descriptorFields = proto.getDescriptorForType().getFields();
        Row row = new Row(descriptorFields.size() + extraColumns);
        for (FieldDescriptor fieldDescriptor : descriptorFields) {
            ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
            row.setField(fieldDescriptor.getIndex(), protoHandler.transformFromKafka(proto.getField(fieldDescriptor)));
        }
        return row;
    }

    /**
     * Create row from specfied proto and extra columns equals to zero.
     *
     * @param proto the proto
     * @return the row
     */
    public static Row createRow(DynamicMessage proto) {
        return createRow(proto, 0);
    }
}
