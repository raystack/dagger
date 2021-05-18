package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

public enum ValueEnum {
    BooleanType(6),
    ByteType(0),
    DoubleType(4),
    FloatType(5),
    IntegerType(2),
    LongType(3),
    StringType(1),
    TimestampType(7);

    private Integer value;

    ValueEnum(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
