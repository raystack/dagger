package com.gotocompany.dagger.common.serde.typehandler.repeated;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.serde.parquet.SimpleGroupValidation;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandler;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;

/**
 * The type Repeated enum proto handler.
 */
public class RepeatedEnumHandler implements TypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private static final Gson GSON = new Gson();

    /**
     * Instantiates a new Repeated enum proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public RepeatedEnumHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == ENUM && fieldDescriptor.isRepeated();
    }

    @Override
    public DynamicMessage.Builder transformToProtoBuilder(DynamicMessage.Builder builder, Object field) {
        return builder;
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        return getValue(field);
    }

    @Override
    public Object transformFromProto(Object field) {
        return getValue(field);
    }

    @Override
    public Object transformFromProtoUsingCache(Object field, FieldDescriptorCache cache) {
        return getValue(field);
    }

    @Override
    public Object transformFromParquet(SimpleGroup simpleGroup) {
        String defaultEnumValue = fieldDescriptor.getEnumType().findValueByNumber(0).getName();
        List<String> enumArrayList = new ArrayList<>();
        String fieldName = fieldDescriptor.getName();
        if (simpleGroup != null && SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            int repetitionCount = simpleGroup.getFieldRepetitionCount(fieldName);
            for (int positionIndex = 0; positionIndex < repetitionCount; positionIndex++) {
                String extractedValue = simpleGroup.getString(fieldName, positionIndex);
                Descriptors.EnumValueDescriptor enumValueDescriptor = fieldDescriptor.getEnumType().findValueByName(extractedValue);
                String enumValue = enumValueDescriptor == null ? defaultEnumValue : enumValueDescriptor.getName();
                enumArrayList.add(enumValue);
            }
        }
        return enumArrayList.toArray(new String[]{});
    }

    @Override
    public Object transformToJson(Object field) {
        return GSON.toJson(getValue(field));
    }

    @Override
    public TypeInformation getTypeInformation() {
        return ObjectArrayTypeInfo.getInfoFor(Types.STRING);
    }

    private Object getValue(Object field) {
        List<String> values = new ArrayList<>();
        if (field != null) {
            values = getStringRow((List) field);
        }
        return values.toArray(new String[]{});
    }

    private List<String> getStringRow(List<Object> protos) {
        return protos
                .stream()
                .map(String::valueOf)
                .collect(Collectors.toList());
    }
}
