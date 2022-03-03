package io.odpf.dagger.common.serde.proto.protohandler;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import io.odpf.dagger.common.serde.parquet.parser.primitive.*;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestFeedbackLogMessage;
import io.odpf.dagger.consumer.TestNestedRepeatedMessage;
import io.odpf.dagger.consumer.TestRepeatedEnumMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.junit.Assert.assertEquals;

public class ProtoHandlerFactoryTest {
    @Before
    public void setup() {
        ProtoHandlerFactory.clearProtoHandlerMap();
        ProtoHandlerFactory.clearParquetDataTypeParserMap();
    }

    @Test
    public void shouldReturnMapProtoHandlerIfMapFieldDescriptorPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(mapFieldDescriptor);
        assertEquals(MapProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnTimestampProtoHandlerIfTimestampFieldDescriptorPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(timestampFieldDescriptor);
        assertEquals(TimestampProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnEnumProtoHandlerIfEnumFieldDescriptorPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(enumFieldDescriptor);
        assertEquals(EnumProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedProtoHandlerIfRepeatedFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(repeatedFieldDescriptor);
        assertEquals(RepeatedPrimitiveProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedMessageProtoHandlerIfRepeatedMessageFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor);
        assertEquals(RepeatedMessageProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedEnumProtoHandlerIfRepeatedEnumFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(repeatedEnumFieldDescriptor);
        assertEquals(RepeatedEnumProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedStructProtoHandlerIfRepeatedStructFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(repeatedStructFieldDescriptor);
        assertEquals(RepeatedStructMessageProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnStructProtoHandlerIfStructFieldDescriptorPassed() {
        Descriptors.FieldDescriptor structFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(structFieldDescriptor);
        assertEquals(StructMessageProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnMessageProtoHandlerIfMessageFieldDescriptorPassed() {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(messageFieldDescriptor);
        assertEquals(MessageProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnDefaultProtoHandlerIfPrimitiveFieldDescriptorPassed() {
        Descriptors.FieldDescriptor primitiveFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnTheSameObjectWithMultipleThreads() throws InterruptedException {
        ExecutorService e = Executors.newFixedThreadPool(100);
        final ProtoHandler[] cache = {null};
        for (int i = 0; i < 1000; i++) {
            e.submit(() -> {
                Descriptors.FieldDescriptor primitiveFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
                assertEquals(PrimitiveProtoHandler.class, protoHandler.getClass());
                synchronized (cache) {
                    ProtoHandler oldHandler = cache[0];
                    if (oldHandler != null) {
                        assertEquals(protoHandler, cache[0]);
                    } else {
                        // Only one thread will set this
                        cache[0] = protoHandler;
                    }
                }
            });
        }
        e.shutdown();
        e.awaitTermination(10000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void shouldReturnTheSameObjectWhenFactoryMethodIsCalledMultipleTimes() {
        Descriptors.FieldDescriptor primitiveFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveProtoHandler.class, protoHandler.getClass());
        ProtoHandler newProtoHandler = ProtoHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveProtoHandler.class, newProtoHandler.getClass());
        assertEquals(protoHandler, newProtoHandler);
    }

    @Test(expected = DaggerDeserializationException.class)
    public void getParquetDataTypeParserShouldThrowExceptionWhenSimpleGroupPassedIsNull() {
        ProtoHandlerFactory.getParquetDataTypeParser(null, "column-with-binary-string-type");
    }

    @Test(expected = DaggerDeserializationException.class)
    public void getParquetDataTypeParserShouldThrowExceptionWhenSimpleGroupPassedDoesNotContainTheField() {
        GroupType parquetSchema = Types.requiredGroup()
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "column-with-binary-string-type");
    }

    @Test(expected = DaggerDeserializationException.class)
    public void getParquetDataTypeParserShouldThrowExceptionWhenFieldNameIsNotAPrimitiveType() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .optionalGroup()
                .required(BOOLEAN).named("boolean-nested-column")
                .named("subgroup-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "subgroup-column");
    }

    @Test(expected = DaggerDeserializationException.class)
    public void getParquetDataTypeParserShouldThrowExceptionWhenNoSuitableParserCouldBeFound() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT96).named("some-random-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-random-column");
    }

    @Test
    public void getParquetDataTypeParserShouldReturnParquetBooleanParserIfBooleanFieldIsPassed() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(BOOLEAN).named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetBooleanParser.class, parquetDataTypeParser.getClass());
    }

    @Test
    public void getParquetDataTypeParserShouldReturnParquetDoubleParserIfDoubleFieldIsPassed() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(DOUBLE).named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetDoubleParser.class, parquetDataTypeParser.getClass());
    }

    @Test
    public void getParquetDataTypeParserShouldReturnParquetInt32ParserIfInt32FieldIsPassed() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(INT32).named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetInt32Parser.class, parquetDataTypeParser.getClass());
    }

    @Test
    public void getParquetDataTypeParserShouldReturnParquetFloatParserIfFloatFieldIsPassed() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(FLOAT).named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetFloatParser.class, parquetDataTypeParser.getClass());
    }

    @Test
    public void getParquetDataTypeParserShouldReturnParquetInt64ParserIfInt64FieldIsPassed() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(INT64).named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetInt64Parser.class, parquetDataTypeParser.getClass());
    }

    @Test
    public void getParquetDataTypeParserShouldReturnParquetBinaryEnumParserIfBinaryEnumFieldIsPassed() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(BINARY).as(LogicalTypeAnnotation.enumType()).named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetBinaryEnumParser.class, parquetDataTypeParser.getClass());
    }

    @Test
    public void getParquetDataTypeParserShouldReturnParquetBinaryStringParserIfBinaryStringFieldIsPassed() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(BINARY).as(LogicalTypeAnnotation.stringType()).named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetBinaryStringParser.class, parquetDataTypeParser.getClass());
    }

    @Test
    public void getParquetDataTypeParserShouldReturnParquetTimestampParserIfInt64TimestampFieldIsPassed() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(INT64).as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetTimestampParser.class, parquetDataTypeParser.getClass());
    }

    @Test
    public void getParquetDataTypeParserShouldReturnTheSameObjectWithMultipleThreads() throws InterruptedException {
        ExecutorService e = Executors.newFixedThreadPool(100);
        final ArrayList<AssertionError> assertionErrors = new ArrayList<>();
        final ParquetDataTypeParser[] cache = {null};

        for (int i = 0; i < 1000; i++) {
            e.submit(() -> {
                try {
                    GroupType parquetSchema = Types
                            .requiredGroup()
                            .required(FLOAT).named("some-column")
                            .named("TestGroupType");
                    SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
                    ParquetDataTypeParser parquetDataTypeParser = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");
                    assertEquals(ParquetFloatParser.class, parquetDataTypeParser.getClass());
                    synchronized (cache) {
                        ParquetDataTypeParser oldParser = cache[0];
                        if (oldParser != null) {
                            assertEquals(parquetDataTypeParser, cache[0]);
                        } else {
                            // Only one thread will set this
                            cache[0] = parquetDataTypeParser;
                        }
                    }
                } catch (AssertionError ex) {
                    /* Add any thrown exceptions above to the error list */
                    assertionErrors.add(ex);
                }
            });
        }
        e.shutdown();
        e.awaitTermination(10000, TimeUnit.MILLISECONDS);

        if(!assertionErrors.isEmpty()) {
            String[] errors = assertionErrors.stream()
                    .map(Throwable::toString)
                    .toArray(String[]::new);
            throw new AssertionError(String.join("\n", errors));
        }
    }

    @Test
    public void getParquetDataTypeParserShouldReturnTheSameObjectWhenFactoryMethodIsCalledMultipleTimes() {
        GroupType parquetSchema = Types
                .requiredGroup()
                .required(BOOLEAN).named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetDataTypeParser parquetDataTypeParser1 = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");
        ParquetDataTypeParser parquetDataTypeParser2 = ProtoHandlerFactory.getParquetDataTypeParser(simpleGroup, "some-column");

        assertEquals(ParquetBooleanParser.class, parquetDataTypeParser1.getClass());
        assertEquals(parquetDataTypeParser1, parquetDataTypeParser2);
    }
}