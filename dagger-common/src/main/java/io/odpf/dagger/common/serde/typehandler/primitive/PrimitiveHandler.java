package io.odpf.dagger.common.serde.typehandler.primitive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.parquet.example.data.simple.SimpleGroup;

/**
 * The interface Primitive type handler.
 */
public interface PrimitiveHandler {
    /**
     * this method check if it can handle the data type or not.
     *
     * @return the boolean
     */
    boolean canHandle();

    /**
     * Parses a primitive value from an object.
     *
     * @param field the object to be parsed
     * @return the parsed value
     */
    Object parseObject(Object field);

    /**
     * Parses a primitive value from a SimpleGroup object.
     *
     * @param simpleGroup SimpleGroup object to be parsed
     * @return the parsed value
     */
    Object parseSimpleGroup(SimpleGroup simpleGroup);

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
