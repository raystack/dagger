package com.gotocompany.dagger.common.serde.typehandler.complex;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandler;
import com.gotocompany.dagger.common.serde.typehandler.TypeInformationFactory;
import com.gotocompany.dagger.common.serde.typehandler.repeated.RepeatedMessageHandler;
import com.gotocompany.dagger.common.serde.typehandler.RowFactory;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandlerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.gotocompany.dagger.common.serde.parquet.SimpleGroupValidation.checkFieldExistsAndIsInitialized;
import static com.gotocompany.dagger.common.serde.parquet.SimpleGroupValidation.checkIsLegacySimpleGroupMap;
import static com.gotocompany.dagger.common.serde.parquet.SimpleGroupValidation.checkIsStandardSimpleGroupMap;

/**
 * The type Map proto handler.
 */
public class MapHandler implements TypeHandler {

    private Descriptors.FieldDescriptor fieldDescriptor;
    private TypeHandler repeatedMessageHandler;

    /**
     * Instantiates a new Map proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public MapHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
        this.repeatedMessageHandler = new RepeatedMessageHandler(fieldDescriptor);
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.isMapField();
    }

    @Override
    public DynamicMessage.Builder transformToProtoBuilder(DynamicMessage.Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }
        if (field instanceof Map) {
            Map<?, ?> mapField = (Map<?, ?>) field;
            ArrayList<Row> rows = new ArrayList<>();
            for (Entry<?, ?> entry : mapField.entrySet()) {
                rows.add(Row.of(entry.getKey(), entry.getValue()));
            }
            return repeatedMessageHandler.transformToProtoBuilder(builder, rows.toArray());
        }
        return repeatedMessageHandler.transformToProtoBuilder(builder, field);
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        ArrayList<Row> rows = new ArrayList<>();
        if (field == null) {
            return rows.toArray();
        }
        if (field instanceof Map) {
            Map<String, ?> mapField = (Map<String, ?>) field;
            for (Entry<String, ?> entry : mapField.entrySet()) {
                Descriptors.FieldDescriptor keyDescriptor = fieldDescriptor.getMessageType().findFieldByName("key");
                Descriptors.FieldDescriptor valueDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
                TypeHandler handler = TypeHandlerFactory.getTypeHandler(keyDescriptor);
                Object key = handler.transformFromPostProcessor(entry.getKey());
                Object value = TypeHandlerFactory.getTypeHandler(valueDescriptor).transformFromPostProcessor(entry.getValue());
                rows.add(Row.of(key, value));
            }
            return rows.toArray();
        }
        if (field instanceof List) {
            return repeatedMessageHandler.transformFromPostProcessor(field);
        }
        return rows.toArray();
    }

    @Override
    public Object transformFromProto(Object field) {
        return repeatedMessageHandler.transformFromProto(field);
    }

    @Override
    public Object transformFromProtoUsingCache(Object field, FieldDescriptorCache cache) {
        return repeatedMessageHandler.transformFromProtoUsingCache(field, cache);
    }

    @Override
    public Object transformFromParquet(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        if (simpleGroup != null && checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            if (checkIsLegacySimpleGroupMap(simpleGroup, fieldName)) {
                return transformLegacyMapFromSimpleGroup(simpleGroup, fieldName);
            } else if (checkIsStandardSimpleGroupMap(simpleGroup, fieldName)) {
                return transformStandardMapFromSimpleGroup(simpleGroup, fieldName);
            }
        }
        return new Row[0];
    }

    private Row[] transformLegacyMapFromSimpleGroup(SimpleGroup simpleGroup, String fieldName) {
        ArrayList<Row> deserializedRows = new ArrayList<>();
        int repetitionCount = simpleGroup.getFieldRepetitionCount(fieldName);
        Descriptors.Descriptor keyValueDescriptor = fieldDescriptor.getMessageType();
        for (int i = 0; i < repetitionCount; i++) {
            SimpleGroup keyValuePair = (SimpleGroup) simpleGroup.getGroup(fieldName, i);
            deserializedRows.add(RowFactory.createRow(keyValueDescriptor, keyValuePair));
        }
        return deserializedRows.toArray(new Row[]{});
    }

    private Row[] transformStandardMapFromSimpleGroup(SimpleGroup simpleGroup, String fieldName) {
        ArrayList<Row> deserializedRows = new ArrayList<>();
        final String innerFieldName = "key_value";
        SimpleGroup nestedMapGroup = (SimpleGroup) simpleGroup.getGroup(fieldName, 0);
        int repetitionCount = nestedMapGroup.getFieldRepetitionCount(innerFieldName);
        Descriptors.Descriptor keyValueDescriptor = fieldDescriptor.getMessageType();
        for (int i = 0; i < repetitionCount; i++) {
            SimpleGroup keyValuePair = (SimpleGroup) nestedMapGroup.getGroup(innerFieldName, i);
            deserializedRows.add(RowFactory.createRow(keyValueDescriptor, keyValuePair));
        }
        return deserializedRows.toArray(new Row[]{});
    }

    @Override
    public Object transformToJson(Object field) {
        return null;
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.OBJECT_ARRAY(TypeInformationFactory.getRowType(fieldDescriptor.getMessageType()));
    }
}
