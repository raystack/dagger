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
     * Parses a repeated object into an array of Java primitives.
     *
     * @param field the object array
     * @return array of java primitive values as obtained from the repeated object
     */
    Object parseRepeatedObjectField(Object field);

    /**
     * Extracts the specific repeated primitive field from a SimpleGroup object and
     * transforms it into an array of Java primitives.
     *
     * @param simpleGroup SimpleGroup object inside which the repeated primitive field resides
     * @return array of java primitive values as obtained from the repeated primitive field
     */
    Object parseRepeatedSimpleGroupField(SimpleGroup simpleGroup);

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
