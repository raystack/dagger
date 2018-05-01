package com.gojek.daggers;

import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.Map;

public class DescriptorStore {
    public static final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class is misconfigured";
    private static Map<String, Descriptors.Descriptor> descriptorMap = new HashMap<>();

    public static Descriptors.Descriptor get(String className) {
        Descriptors.Descriptor desc = descriptorMap.get(className);
        if (desc == null) {
            try {
                Class<?> protoClass = Class.forName(className);
                desc = (Descriptors.Descriptor) protoClass.getMethod("getDescriptor")
                        .invoke(null);
                descriptorMap.put(className, desc);
            } catch (ReflectiveOperationException e) {
                throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR, e);
            }
        }
        return desc;
    }
}
