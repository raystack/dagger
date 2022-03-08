package io.odpf.dagger.common.serde.proto.protohandler;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import io.odpf.dagger.common.exceptions.serde.InvalidDataTypeException;
import io.odpf.dagger.common.serde.parquet.parser.validation.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

/**
 * The type Timestamp proto handler.
 */
public class TimestampProtoHandler implements ProtoHandler {
    private static final int SECOND_TO_MS_FACTOR = 1000;
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Timestamp proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public TimestampProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp");
    }

    @Override
    public DynamicMessage.Builder transformForKafka(DynamicMessage.Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }
        Timestamp timestamp = null;
        if (field instanceof java.sql.Timestamp) {
            timestamp = convertSqlTimestamp((java.sql.Timestamp) field);
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
    public Object transformFromKafka(Object field) {
        return RowFactory.createRow((DynamicMessage) field);
    }

    @Override
    public Object transformFromParquet(Object field) {
        if (!(field instanceof SimpleGroup)) {
            String errMessage = String.format("Could not extract timestamp with descriptor name %s from %s",
                    fieldDescriptor.getName(), field);
            throw new IllegalArgumentException(errMessage);
        }
        return transformFromSimpleGroup((SimpleGroup)field);
    }

    private Object transformFromSimpleGroup(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            DynamicMessage dynamicMessage =  transformParquetInt64MillisToDynamicMessage(fieldName, simpleGroup);
            return RowFactory.createRow(dynamicMessage);
        }
        String errMessage = String.format("Could not extract timestamp with descriptor name %s from simple group of type: %s",
                fieldDescriptor.getName(), simpleGroup.getType().toString());
        throw new InvalidDataTypeException(errMessage);
    }

    /* Handling for latest parquet files in which timestamps are encoded as int64 epoch milliseconds with logical type
    annotation of TIMESTAMP(true, MILLIS) */
    private DynamicMessage transformParquetInt64MillisToDynamicMessage(String fieldName, SimpleGroup simpleGroup) {
        long timeInMillis = simpleGroup.getLong(fieldName, 0);
        Timestamp protoTimestamp = transformParquetInt64MillisToProtoTimestamp(timeInMillis);
        return transformProtoTimestampToDynamicMessage(protoTimestamp);
    }

    private Timestamp transformParquetInt64MillisToProtoTimestamp(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private DynamicMessage transformProtoTimestampToDynamicMessage(Timestamp protoTimestamp) {
        try {
            /* getting the descriptor from fieldDescriptor to create the timestamp message. This will ensure that the
            nested row for timestamp is created with the same position numbers as the fieldDescriptor indices of
            the seconds+nanos fields when RowFactory.createRow() is called above */
            return DynamicMessage.parseFrom(fieldDescriptor.getMessageType(), protoTimestamp.toByteArray());
        } catch (InvalidProtocolBufferException ex) {
            throw new DaggerDeserializationException("Error encountered while creating dynamic message from timestamp");
        }
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
