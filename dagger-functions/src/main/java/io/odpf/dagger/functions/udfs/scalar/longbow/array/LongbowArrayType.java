package io.odpf.dagger.functions.udfs.scalar.longbow.array;

import io.odpf.dagger.functions.exceptions.ArrayAggregationException;
import java.io.Serializable;
import java.util.function.Function;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * The enum Data type.
 */
public enum LongbowArrayType implements Serializable {
    INTEGER((Stream<Object> stream) -> (stream.mapToInt(Integer.class::cast))),
    INT((Stream<Object> stream) -> (stream.mapToInt(Integer.class::cast))),
    DOUBLE((Stream<Object> stream) -> (stream.mapToDouble(Double.class::cast))),
    FLOAT((Stream<Object> stream) -> (stream.mapToDouble(Float.class::cast))),
    LONG((Stream<Object> stream) -> (stream.mapToLong(Long.class::cast))),
    BIGINT((Stream<Object> stream) -> (stream.mapToLong(Long.class::cast))),
    OTHER((Stream<Object> stream) -> (stream));

    private Function<Stream<Object>, BaseStream> inputCastingFunction;

    LongbowArrayType(Function<Stream<Object>, BaseStream> inputCastingFunction) {
        this.inputCastingFunction = inputCastingFunction;
    }

    /**
     * Gets data type.
     *
     * @param inputDataType the input data type
     * @return the data type
     */
    public static LongbowArrayType getDataType(String inputDataType) {
        String typeInUpperCaseCase = inputDataType.toUpperCase();
        try {
            return LongbowArrayType.valueOf(typeInUpperCaseCase);
        } catch (IllegalArgumentException e) {
            throw new ArrayAggregationException("No support for inputDataType: "
                    + inputDataType
                    + ".Please provide 'Other' as inputDataType instead.");
        }
    }

    /**
     * Gets input casting function.
     *
     * @return the input casting function
     */
    public Function<Stream<Object>, BaseStream> getInputCastingFunction() {
        return inputCastingFunction;
    }

}
