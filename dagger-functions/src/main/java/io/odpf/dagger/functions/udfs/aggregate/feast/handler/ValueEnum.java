package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

/**
 * The enum Value.
 */
public enum ValueEnum {
    /**
     * Boolean type value enum.
     */
    BooleanType(6),
    /**
     * Byte type value enum.
     */
    ByteType(0),
    /**
     * Double type value enum.
     */
    DoubleType(4),
    /**
     * Float type value enum.
     */
    FloatType(5),
    /**
     * Integer type value enum.
     */
    IntegerType(2),
    /**
     * Long type value enum.
     */
    LongType(3),
    /**
     * String type value enum.
     */
    StringType(1),
    /**
     * Timestamp type value enum.
     */
    TimestampType(7);

    private Integer value;

    ValueEnum(Integer value) {
        this.value = value;
    }

    /**
     * Gets value.
     *
     * @return the value
     */
    public Integer getValue() {
        return value;
    }
}
