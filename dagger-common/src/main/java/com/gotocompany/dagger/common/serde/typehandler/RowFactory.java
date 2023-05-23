package com.gotocompany.dagger.common.serde.typehandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import org.apache.flink.types.Row;
import org.apache.parquet.example.data.simple.SimpleGroup;

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
            TypeHandler typeHandler = TypeHandlerFactory.getTypeHandler(fieldDescriptor);
            if (inputMap.get(fieldDescriptor.getName()) != null) {
                row.setField(fieldDescriptor.getIndex(), typeHandler.transformFromPostProcessor(inputMap.get(fieldDescriptor.getName())));
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
            TypeHandler typeHandler = TypeHandlerFactory.getTypeHandler(fieldDescriptor);
            row.setField(fieldDescriptor.getIndex(), typeHandler.transformFromProto(proto.getField(fieldDescriptor)));
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
    public static Row createRow(DynamicMessage proto, int extraColumns, FieldDescriptorCache cache) {
        List<FieldDescriptor> descriptorFields = proto.getDescriptorForType().getFields();
        int fieldCount = cache.getOriginalFieldCount(proto.getDescriptorForType());

        Row row = new Row(fieldCount + extraColumns);
        for (FieldDescriptor fieldDescriptor : descriptorFields) {

            if (!cache.containsField(fieldDescriptor.getFullName())) {
                continue;
            }

            TypeHandler typeHandler = TypeHandlerFactory.getTypeHandler(fieldDescriptor);
            row.setField(cache.getOriginalFieldIndex(fieldDescriptor), typeHandler.transformFromProtoUsingCache(proto.getField(fieldDescriptor), cache));
        }
        return row;
    }


    public static Row createRow(Descriptors.Descriptor descriptor, SimpleGroup simpleGroup, int extraColumns) {
        List<FieldDescriptor> descriptorFields = descriptor.getFields();
        Row row = new Row(descriptorFields.size() + extraColumns);
        for (FieldDescriptor fieldDescriptor : descriptorFields) {
            TypeHandler typeHandler = TypeHandlerFactory.getTypeHandler(fieldDescriptor);
            row.setField(fieldDescriptor.getIndex(), typeHandler.transformFromParquet(simpleGroup));
        }
        return row;
    }

    public static Row createRow(Descriptors.Descriptor descriptor, SimpleGroup simpleGroup) {
        return createRow(descriptor, simpleGroup, 0);
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

    /**
     * Create row from specfied proto and extra columns equals to zero.
     *
     * @param proto the proto
     * @return the row
     */
    public static Row createRow(DynamicMessage proto, FieldDescriptorCache cache) {
        return createRow(proto, 0, cache);
    }

}
