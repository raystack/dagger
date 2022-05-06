package io.odpf.dagger.common.serde.typehandler.complex;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MapEntry;
import com.google.protobuf.WireFormat;
import io.odpf.dagger.common.serde.typehandler.TypeHandler;
import io.odpf.dagger.common.serde.typehandler.RowFactory;
import io.odpf.dagger.common.serde.typehandler.TypeInformationFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static io.odpf.dagger.common.serde.parquet.SimpleGroupValidation.checkFieldExistsAndIsInitialized;
import static io.odpf.dagger.common.serde.parquet.SimpleGroupValidation.checkIsLegacySimpleGroupMap;
import static io.odpf.dagger.common.serde.parquet.SimpleGroupValidation.checkIsStandardSimpleGroupMap;

/**
 * The type Map proto handler.
 */
public class MapHandler implements TypeHandler {

    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Map proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public MapHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
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
            convertFromMap(builder, (Map<String, String>) field);
        }

        if (field instanceof Object[]) {
            convertFromRow(builder, (Object[]) field);
        }

        return builder;
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        ArrayList<Row> rows = new ArrayList<>();
        if (field != null) {
            Map<String, String> mapField = (Map<String, String>) field;
            for (Entry<String, String> entry : mapField.entrySet()) {
                rows.add(getRowFromMap(entry));
            }
        }
        return rows.toArray();
    }

    @Override
    public Object transformFromProto(Object field) {
        ArrayList<Row> rows = new ArrayList<>();
        if (field != null) {
            List<DynamicMessage> protos = (List<DynamicMessage>) field;
            protos.forEach(proto -> rows.add(getRowFromMap(proto)));
        }
        return rows.toArray();
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

    private Row getRowFromMap(Entry<String, String> entry) {
        Row row = new Row(2);
        row.setField(0, entry.getKey());
        row.setField(1, entry.getValue());
        return row;
    }

    private Row getRowFromMap(DynamicMessage proto) {
        Row row = new Row(2);
        row.setField(0, parse(proto, "key"));
        row.setField(1, parse(proto, "value"));
        return row;
    }

    private Object parse(DynamicMessage proto, String fieldName) {
        Object field = proto.getField(proto.getDescriptorForType().findFieldByName(fieldName));
        if (DynamicMessage.class.equals(field.getClass())) {
            field = RowFactory.createRow((DynamicMessage) field);
        }
        return field;
    }

    private void convertFromRow(DynamicMessage.Builder builder, Object[] field) {
        for (Object inputValue : field) {
            Row inputRow = (Row) inputValue;
            if (inputRow.getArity() != 2) {
                throw new IllegalArgumentException("Row: " + inputRow.toString() + " of size: " + inputRow.getArity() + " cannot be converted to map");
            }
            MapEntry<String, String> mapEntry = MapEntry
                    .newDefaultInstance(fieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
            builder.addRepeatedField(fieldDescriptor,
                    mapEntry.toBuilder()
                            .setKey((String) inputRow.getField(0))
                            .setValue((String) inputRow.getField(1))
                            .buildPartial());
        }
    }

    private void convertFromMap(DynamicMessage.Builder builder, Map<String, String> field) {
        for (Entry<String, String> entry : field.entrySet()) {
            MapEntry<String, String> mapEntry = MapEntry.newDefaultInstance(fieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
            builder.addRepeatedField(fieldDescriptor,
                    mapEntry.toBuilder()
                            .setKey(entry.getKey())
                            .setValue(entry.getValue())
                            .buildPartial());
        }
    }
}
