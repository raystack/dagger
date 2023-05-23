package com.gotocompany.dagger.common.core;

import com.google.protobuf.Descriptors;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FieldDescriptorCache implements Serializable {
    private final Map<String, Integer> fieldDescriptorIndexMap = new HashMap<>();
    private final Map<String, Integer> protoDescriptorArityMap = new HashMap<>();

    public FieldDescriptorCache(Descriptors.Descriptor descriptor) {

        cacheFieldDescriptorMap(descriptor);
    }

    public void cacheFieldDescriptorMap(Descriptors.Descriptor descriptor) {

        if (protoDescriptorArityMap.containsKey(descriptor.getFullName())) {
            return;
        }
        List<Descriptors.FieldDescriptor> descriptorFields = descriptor.getFields();
        protoDescriptorArityMap.putIfAbsent(descriptor.getFullName(), descriptorFields.size());

        for (Descriptors.FieldDescriptor fieldDescriptor : descriptorFields) {
            fieldDescriptorIndexMap.putIfAbsent(fieldDescriptor.getFullName(), fieldDescriptor.getIndex());
        }

        for (Descriptors.FieldDescriptor fieldDescriptor : descriptorFields) {
            if (fieldDescriptor.getType().toString().equals("MESSAGE")) {
                cacheFieldDescriptorMap(fieldDescriptor.getMessageType());

            }
        }
    }

    public int getOriginalFieldIndex(Descriptors.FieldDescriptor fieldDescriptor) {
        if (!fieldDescriptorIndexMap.containsKey(fieldDescriptor.getFullName())) {
            throw new IllegalArgumentException("The Field Descriptor " + fieldDescriptor.getFullName() + " was not found in the cache");
        }
        return fieldDescriptorIndexMap.get(fieldDescriptor.getFullName());
    }

    public boolean containsField(String fieldName) {

        return fieldDescriptorIndexMap.containsKey(fieldName);
    }

    public int getOriginalFieldCount(Descriptors.Descriptor descriptor) {
        if (!protoDescriptorArityMap.containsKey(descriptor.getFullName())) {
            throw new IllegalArgumentException("The Proto Descriptor " + descriptor.getFullName() + " was not found in the cache");
        }
        return protoDescriptorArityMap.get(descriptor.getFullName());
    }
}
