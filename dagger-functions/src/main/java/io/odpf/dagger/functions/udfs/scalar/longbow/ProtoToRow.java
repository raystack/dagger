package io.odpf.dagger.functions.udfs.scalar.longbow;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The class that handle to get Row from Proto for Longbow plus.
 */
public class ProtoToRow implements Serializable {

    /**
     * Gets row.
     *
     * @param proto the proto
     * @return the row
     */
    public Row getRow(DynamicMessage proto) {
        List<Descriptors.FieldDescriptor> fields = proto.getDescriptorForType().getFields();
        Row row = new Row(fields.size());
        for (Descriptors.FieldDescriptor field : fields) {
            if (field.isMapField()) {
                List<DynamicMessage> mapEntries = (List<DynamicMessage>) proto.getField(field);
                row.setField(field.getIndex(), getMapRow(mapEntries));
                continue;
            }
            if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                if (field.isRepeated()) {
                    row.setField(field.getIndex(), getStringRow(((List) proto.getField(field))));
                } else {
                    row.setField(field.getIndex(), proto.getField(field).toString());
                }
            } else if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                if (field.toProto().getTypeName().equals(".google.protobuf.Struct")) {
                    continue;
                }
                if (field.isRepeated()) {
                    row.setField(field.getIndex(), getRow((List<DynamicMessage>) proto.getField(field)));
                } else {
                    row.setField(field.getIndex(), getRow((DynamicMessage) proto.getField(field)));
                }
            } else {
                if (field.isRepeated()) {
                    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
                        row.setField(field.getIndex(), getRowForStringList((List<String>) proto.getField(field)));
                    } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.BYTE_STRING) {
                        row.setField(field.getIndex(), getRowForByteString((List<ByteString>) proto.getField(field)));
                    }
                } else {
                    row.setField(field.getIndex(), proto.getField(field));
                }
            }
        }
        return row;
    }

    /**
     * Get row object.
     *
     * @param protos the protos
     * @return the row object
     */
    public Object[] getRow(List<DynamicMessage> protos) {
        ArrayList<Row> rows = new ArrayList<>();
        protos.forEach(generatedMessageV3 -> rows.add(getRow(generatedMessageV3)));
        return rows.toArray();
    }

    /**
     * Get string row.
     *
     * @param protos the protos
     * @return the string row
     */
    public String[] getStringRow(List<Object> protos) {
        return protos
                .stream()
                .map(String::valueOf)
                .toArray(String[]::new);
    }

    private Object[] getMapRow(List<DynamicMessage> protos) {
        ArrayList<Row> rows = new ArrayList<>();
        protos.forEach(entry -> rows.add(getRowFromMap(entry)));
        return rows.toArray();
    }

    private Row getRowFromMap(DynamicMessage protos) {
        Row row = new Row(2);
        Object[] keyValue = protos.getAllFields().values().toArray();
        row.setField(0, keyValue.length > 0 ? keyValue[0] : "");
        row.setField(1, keyValue.length > 1 ? keyValue[1] : "");
        return row;
    }

    /**
     * Get row for string list.
     *
     * @param listField the list field
     * @return the row for string list
     */
    public String[] getRowForStringList(List<String> listField) {
        String[] list = new String[listField.size()];
        for (int listIndex = 0; listIndex < listField.size(); listIndex++) {
            list[listIndex] = listField.get(listIndex);
        }
        return list;
    }

    private ByteString[] getRowForByteString(List<ByteString> listField) {
        ByteString[] byteStrings = new ByteString[listField.size()];
        for (int listIndex = 0; listIndex < listField.size(); listIndex++) {
            byteStrings[listIndex] = listField.get(listIndex);
        }
        return byteStrings;
    }

}
