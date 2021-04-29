package io.odpf.dagger.core.protohandler.typehandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public interface PrimitiveTypeHandler {
    boolean canHandle();

    Object getValue(Object field);

    Object getArray(Object field);

    TypeInformation getTypeInformation();

    TypeInformation getArrayType();

    default String getValueOrDefault(Object input, String defaultValue) {
        return input == null ? defaultValue : input.toString();
    }
}
