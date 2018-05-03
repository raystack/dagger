package com.gojek.daggers;

import com.gojek.de.stencil.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import com.google.protobuf.Descriptors;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

public class DescriptorStore {
    public static final String PROTO_CLASS_MISCONFIGURED_ERROR = "proto class is misconfigured";
    private static Map<String, Descriptors.Descriptor> descriptorMap = new HashMap<>();
    private static StencilClient client;

    public static void load(Configuration configuration) {
        if ("true".equals(configuration.getString("ENABLE_STENCIL_URL", "false"))) {
            client = StencilClientFactory.getClient(
                    configuration.getString("STENCIL_URL", ""),
                    configuration.toMap()
            );
        } else {
            client = StencilClientFactory.getClient();
        }
        client.load();
    }

    public static Descriptors.Descriptor get(String className) {
        Descriptors.Descriptor dsc = client.get(className);
        if (dsc == null) {
            throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR);
        }
        return dsc;
    }
}
