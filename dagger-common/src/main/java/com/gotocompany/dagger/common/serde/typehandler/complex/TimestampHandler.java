package com.gotocompany.dagger.common.serde.typehandler.complex;

import com.google.protobuf.Timestamp;
import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.serde.parquet.SimpleGroupValidation;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandler;
import com.gotocompany.dagger.common.serde.typehandler.RowFactory;
import com.gotocompany.dagger.common.serde.typehandler.TypeInformationFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

/**
 * The type Timestamp proto handler.
 */
public class TimestampHandler implements TypeHandler {
    private static final int SECOND_TO_MS_FACTOR = 1000;
    private static final long DEFAULT_SECONDS_VALUE = 0L;
    private static final int DEFAULT_NANOS_VALUE = 0;
    private static final int MS_TO_NANOS_FACTOR = 1000_000;
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Timestamp proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public TimestampHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp");
    }

    @Override
    public DynamicMessage.Builder transformToProtoBuilder(DynamicMessage.Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }
        Timestamp timestamp = null;
        if (field instanceof java.sql.Timestamp) {
            timestamp = convertSqlTimestamp((java.sql.Timestamp) field);
        }

        if (field instanceof Instant) {
            timestamp = Timestamp.newBuilder().setSeconds(((Instant) field).getEpochSecond()).build();
        }

        if (field instanceof LocalDateTime) {
            timestamp = convertLocalDateTime((LocalDateTime) field);
        }

        if (field instanceof Row) {
            Row timeField = (Row) field;
            if (timeField.getArity() == 2) {
                timestamp = Timestamp.newBuilder()
                        .setSeconds((Long) timeField.getField(0))
                        .setNanos((int) timeField.getField(1))
                        .build();
            } else {
                throw new IllegalArgumentException("Row: " + timeField.toString() + " of size: " + timeField.getArity() + " cannot be converted to timestamp");
            }
        }

        if (field instanceof String) {
            timestamp = Timestamp.newBuilder().setSeconds(Instant.parse(((String) field)).getEpochSecond()).build();
        }

        if (field instanceof Number) {
            timestamp = Timestamp.newBuilder().setSeconds(((Number) field).longValue()).build();
        }

        if (timestamp != null) {
            builder.setField(fieldDescriptor, timestamp);
        }
        return builder;
    }

    private Timestamp convertLocalDateTime(LocalDateTime timeField) {
        return Timestamp.newBuilder()
                .setSeconds(timeField.toEpochSecond(ZoneOffset.UTC))
                .build();
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        return isValid(field) ? field.toString() : null;
    }

    @Override
    public Object transformFromProto(Object field) {
        return RowFactory.createRow((DynamicMessage) field);
    }

    @Override
    public Object transformFromProtoUsingCache(Object field, FieldDescriptorCache cache) {
        return RowFactory.createRow((DynamicMessage) field, cache);
    }

    @Override
    public Object transformFromParquet(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        if (simpleGroup != null && SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            Type timestampType = simpleGroup.getType().getType(fieldName);
            if (timestampType instanceof PrimitiveType) {
                return parseInt64TimestampFromSimpleGroup(simpleGroup, fieldName);
            } else if (timestampType instanceof GroupType) {
                return parseGroupTypeTimestampFromSimpleGroup(simpleGroup, fieldName);
            }
        }
        return Row.of(DEFAULT_SECONDS_VALUE, DEFAULT_NANOS_VALUE);
    }

    private Row parseInt64TimestampFromSimpleGroup(SimpleGroup simpleGroup, String timestampFieldName) {
        /* conversion from ms to nanos borrowed from Instant.java class and inlined here for performance reasons */
        long timeInMillis = simpleGroup.getLong(timestampFieldName, 0);
        long seconds = Math.floorDiv(timeInMillis, SECOND_TO_MS_FACTOR);
        int mos = (int) Math.floorMod(timeInMillis, SECOND_TO_MS_FACTOR);
        int nanos = mos * MS_TO_NANOS_FACTOR;
        return Row.of(seconds, nanos);
    }

    private Row parseGroupTypeTimestampFromSimpleGroup(SimpleGroup simpleGroup, String timestampFieldName) {
        SimpleGroup timestampGroup = (SimpleGroup) simpleGroup.getGroup(timestampFieldName, 0);
        long seconds = 0L;
        int nanos = 0;
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(timestampGroup, "seconds")) {
            seconds = timestampGroup.getLong("seconds", 0);
        }
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(timestampGroup, "nanos")) {
            nanos = timestampGroup.getInteger("nanos", 0);
        }
        return Row.of(seconds, nanos);
    }

    @Override
    public Object transformToJson(Object field) {
        Row timeField = (Row) field;
        if (timeField.getArity() == 2) {
            java.sql.Timestamp timestamp = new java.sql.Timestamp((Long) timeField.getField(0) * SECOND_TO_MS_FACTOR);
            return dateFormat.format(timestamp);
        } else {
            return field;
        }
    }

    @Override
    public TypeInformation getTypeInformation() {
        return TypeInformationFactory.getRowType(fieldDescriptor.getMessageType());
    }

    private Timestamp convertSqlTimestamp(java.sql.Timestamp field) {
        long timestampSeconds = field.getTime() / SECOND_TO_MS_FACTOR;
        int timestampNanos = field.getNanos();
        return Timestamp.newBuilder()
                .setSeconds(timestampSeconds)
                .setNanos(timestampNanos)
                .build();
    }

    private boolean isValid(Object field) {
        if (field == null) {
            return false;
        }
        try {
            Instant.parse(field.toString());
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }
}
