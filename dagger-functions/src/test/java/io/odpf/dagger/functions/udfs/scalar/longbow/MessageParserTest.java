package io.odpf.dagger.functions.udfs.scalar.longbow;

import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestEnrichedBookingLogMessage;
import io.odpf.dagger.consumer.TestApiLogMessage;
import io.odpf.dagger.consumer.TestBookingStatus;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.odpf.dagger.functions.exceptions.LongbowException;
import org.apache.flink.types.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class MessageParserTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldReadFromSingleField() {
        String orderNumber = "test_order_id";
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber(orderNumber).build();
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage).build();
        MessageParser messageParser = new MessageParser();
        ArrayList<String> keys = new ArrayList<>();
        keys.add(0, "order_number");

        Object value = messageParser.read(dynamicMessage, keys);

        assertEquals(value, orderNumber);
    }

    @Test
    public void shouldReadFromNestedField() {
        long timeStampInSeconds = 100;

        Timestamp timestamp = Timestamp.newBuilder().setSeconds(timeStampInSeconds).build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setEventTimestamp(timestamp).build();
        TestEnrichedBookingLogMessage enrichedBookingLogMessage = TestEnrichedBookingLogMessage.newBuilder().setBookingLog(bookingLogMessage).build();

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(enrichedBookingLogMessage).build();

        MessageParser messageParser = new MessageParser();

        ArrayList<String> keys = new ArrayList<>();
        keys.add(0, "booking_log");
        keys.add(1, "event_timestamp");
        keys.add(2, "seconds");

        Object value = messageParser.read(dynamicMessage, keys);

        assertEquals(value, timeStampInSeconds);
    }

    @Test
    public void shouldThrowIfUnableToFindKey() {
        thrown.expectMessage("Key : event_timestamp1 does not exist in Message io.odpf.dagger.consumer.TestBookingLogMessage");
        thrown.expect(LongbowException.class);
        long timeStampInSeconds = 100;

        Timestamp timestamp = Timestamp.newBuilder().setSeconds(timeStampInSeconds).build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setEventTimestamp(timestamp).build();
        TestEnrichedBookingLogMessage enrichedBookingLogMessage = TestEnrichedBookingLogMessage.newBuilder().setBookingLog(bookingLogMessage).build();

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(enrichedBookingLogMessage).build();

        MessageParser messageParser = new MessageParser();

        ArrayList<String> keys = new ArrayList<>();
        keys.add(0, "booking_log");
        keys.add(1, "event_timestamp1");
        keys.add(2, "seconds");

        messageParser.read(dynamicMessage, keys);
    }

    @Test
    public void shouldReadComplexDataFromNestedField() {
        long timeStampInSeconds = 100;

        Timestamp timestamp = Timestamp.newBuilder().setSeconds(timeStampInSeconds).build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setEventTimestamp(timestamp).build();
        TestEnrichedBookingLogMessage enrichedBookingLogMessage = TestEnrichedBookingLogMessage.newBuilder().setBookingLog(bookingLogMessage).build();

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(enrichedBookingLogMessage).build();

        MessageParser messageParser = new MessageParser();

        ArrayList<String> keys = new ArrayList<>();
        keys.add(0, "booking_log");
        keys.add(1, "event_timestamp");

        Row nestedRow = (Row) messageParser.read(dynamicMessage, keys);

        assertEquals(nestedRow.getField(0), timeStampInSeconds);
        assertEquals(nestedRow.getField(1), 0);
    }

    @Test
    public void shouldConvertEnumToStringField() {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setStatus(TestBookingStatus.Enum.COMPLETED).build();
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage).build();
        MessageParser messageParser = new MessageParser();
        ArrayList<String> keys = new ArrayList<>();
        keys.add(0, "status");

        Object value = messageParser.read(dynamicMessage, keys);
        assertEquals(value, "COMPLETED");
    }

    @Test
    public void shouldHandleParsingForRepeatedString() {
        TestApiLogMessage testApiLogMessage = TestApiLogMessage.newBuilder()
                .addRequestHeadersExtra("EXAMPLE-REGISTERED-DEVICE-01")
                .addRequestHeadersExtra("EXAMPLE-REGISTERED-DEVICE-02")
                .addRequestHeadersExtra("EXAMPLE-REGISTERED-DEVICE-03")
                .addRequestHeadersExtra("EXAMPLE-REGISTERED-DEVICE-04")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(testApiLogMessage).build();
        MessageParser messageParser = new MessageParser();
        ArrayList<String> keys = new ArrayList<>();
        keys.add(0, "request_headers_extra");

        String[] value = (String[]) messageParser.read(dynamicMessage, keys);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-01", value[0]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-02", value[1]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-03", value[2]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-04", value[3]);
    }
}
