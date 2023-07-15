package org.raystack.dagger.functions.udfs.scalar;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Timestamp.Builder;
import org.raystack.dagger.consumer.TestBookingLogMessage;
import org.raystack.dagger.functions.exceptions.LongbowException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.Predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CondEqTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldReturnTrueWhenFieldValueMatchComparison() {
        TestBookingLogMessage testBookingLog = TestBookingLogMessage.newBuilder().setOrderNumber("test_order_number_1").build();

        CondEq condEq = new CondEq();
        Predicate<DynamicMessage> predicate = condEq.eval("order_number", "test_order_number_1");

        assertTrue(predicate.test(DynamicMessage.newBuilder(testBookingLog).build()));
    }

    @Test
    public void shouldReturnFalseWhenFieldValueDoesntMatchComparison() {
        TestBookingLogMessage testBookingLog = TestBookingLogMessage.newBuilder().setOrderNumber("test_order_number_1").build();

        CondEq condEq = new CondEq();
        Predicate<DynamicMessage> predicate = condEq.eval("order_number", "test_order_number_2");

        assertFalse(predicate.test(DynamicMessage.newBuilder(testBookingLog).build()));
    }

    @Test
    public void shouldBeAbleToAcceptPrimitiveTypeAsComparison() {
        TestBookingLogMessage testBookingLog = TestBookingLogMessage.newBuilder().setAmountPaidByCash(10000f).build();

        CondEq condEq = new CondEq();
        Predicate<DynamicMessage> predicate = condEq.eval("amount_paid_by_cash", 10000f);

        assertTrue(predicate.test(DynamicMessage.newBuilder(testBookingLog).build()));
    }

    @Test
    public void shouldBeAbleToCompareNestedFieldName() {
        Builder timestamp = Timestamp.newBuilder().setSeconds(0000000L);
        TestBookingLogMessage testBookingLog = TestBookingLogMessage.newBuilder().setEventTimestamp(timestamp).build();

        CondEq condEq = new CondEq();
        Predicate<DynamicMessage> predicate = condEq.eval("event_timestamp.seconds", 0000000L);

        assertTrue(predicate.test(DynamicMessage.newBuilder(testBookingLog).build()));
    }

    @Test
    public void shouldHandleFieldNameEmpty() {
        thrown.expectMessage("Key :  does not exist in Message org.raystack.dagger.consumer.TestBookingLogMessage");
        thrown.expect(LongbowException.class);
        TestBookingLogMessage testBookingLog = TestBookingLogMessage.newBuilder().build();

        CondEq condEq = new CondEq();
        Predicate<DynamicMessage> predicate = condEq.eval("", "arbitrary");

        assertTrue(predicate.test(DynamicMessage.newBuilder(testBookingLog).build()));
    }

    @Test
    public void shouldHandleFieldNameDoesNotExist() {
        thrown.expectMessage("Key : arbitrary does not exist in Message org.raystack.dagger.consumer.TestBookingLogMessage");
        thrown.expect(LongbowException.class);
        TestBookingLogMessage testBookingLog = TestBookingLogMessage.newBuilder().build();

        CondEq condEq = new CondEq();
        Predicate<DynamicMessage> predicate = condEq.eval("arbitrary", "arbitrary");

        assertTrue(predicate.test(DynamicMessage.newBuilder(testBookingLog).build()));
    }

    @Test
    public void shouldBeAbleToHandleEmptyValue() {
        TestBookingLogMessage testBookingLog = TestBookingLogMessage.newBuilder().build();

        CondEq condEq = new CondEq();
        Predicate<DynamicMessage> predicate = condEq.eval("event_timestamp", 0L);

        assertFalse(predicate.test(DynamicMessage.newBuilder(testBookingLog).build()));
    }
}
