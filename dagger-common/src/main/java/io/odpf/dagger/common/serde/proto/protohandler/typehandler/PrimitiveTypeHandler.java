package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * The interface Primitive type handler.
 */
public interface PrimitiveTypeHandler {
    /**
     * this method check if it can handle the data type or not.
     *
     * @return the boolean
     */
    boolean canHandle();

    /**
     * Gets value.
     *
     * @param field the field
     * @return the value
     */
    Object getValue(Object field);

    /**
     * Gets array.
     *
     * @param field the field
     * @return the array
     */
    Object getArray(Object field);

    /**
     * Gets type information.
     *
     * @return the type information
     */
    TypeInformation getTypeInformation();

    /**
     * Gets array type.
     *
     * @return the array type
     */
    TypeInformation getArrayType();

    /**
     * Default method to get value from input or specified default value if input is null.
     *
     * @param input        the input
     * @param defaultValue the default value
     * @return the value or default
     */
    default String getValueOrDefault(Object input, String defaultValue) {
        return input == null ? defaultValue : input.toString();
    }
}
