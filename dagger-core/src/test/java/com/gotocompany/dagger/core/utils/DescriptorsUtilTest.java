package com.gotocompany.dagger.core.utils;

import com.google.protobuf.Descriptors;
import com.gotocompany.dagger.consumer.TestCustomerLogMessage;
import com.gotocompany.dagger.consumer.TestEnrichedBookingLogMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DescriptorsUtilTest {

    @Test
    public void shouldGetFieldDescriptor() {
        String fieldName = "customer_id";
        Descriptors.Descriptor descriptor = TestCustomerLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = DescriptorsUtil.getFieldDescriptor(descriptor, fieldName);
        assertNotNull(fieldDescriptor);
        assertEquals("customer_id", fieldDescriptor.getName());
    }

    @Test
    public void shouldRunGetNestedFieldDescriptor() {
        String fieldName = "customer_profile.customer_id";
        Descriptors.Descriptor descriptor = TestEnrichedBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = DescriptorsUtil.getFieldDescriptor(descriptor, fieldName);
        assertNotNull(fieldDescriptor);
        assertEquals("customer_id", fieldDescriptor.getName());
    }

    @Test
    public void shouldRunGetNestedFieldColumnsDescriptor() {
        Descriptors.Descriptor parentDescriptor = TestEnrichedBookingLogMessage.getDescriptor();
        String[] nestedFieldNames = {"customer_profile", "customer_id"};
        Descriptors.FieldDescriptor fieldDescriptor = DescriptorsUtil.getNestedFieldDescriptor(parentDescriptor, nestedFieldNames);
        assertNotNull(fieldDescriptor);
        assertEquals("customer_id", fieldDescriptor.getName());
    }

    @Test
    public void shouldGiveNullForInvalidFieldFieldDescriptor() {
        Descriptors.Descriptor descriptor = TestCustomerLogMessage.getDescriptor();
        String nonExistentField = "customer-id";
        Descriptors.FieldDescriptor nonExistentFieldDescriptor = DescriptorsUtil.getFieldDescriptor(descriptor, nonExistentField);
        assertNull(nonExistentFieldDescriptor);
    }

    @Test
    public void shouldGiveNullForInvalidNestedFieldDescriptor() {
        Descriptors.Descriptor parentDescriptor = TestEnrichedBookingLogMessage.getDescriptor();
        String fieldName = "customer_profile.customer-id";
        Descriptors.FieldDescriptor invalidFieldDescriptor = DescriptorsUtil.getFieldDescriptor(parentDescriptor, fieldName);
        assertNull(invalidFieldDescriptor);
    }

    @Test
    public void shouldGiveNullForInvlidNestedFieldColumnsDescriptor() {
        Descriptors.Descriptor parentDescriptor = TestEnrichedBookingLogMessage.getDescriptor();
        String[] invalidNestedFieldNames = {"customer_profile", "customer-id"};
        Descriptors.FieldDescriptor invalidFieldDescriptor = DescriptorsUtil.getNestedFieldDescriptor(parentDescriptor, invalidNestedFieldNames);
        assertNull(invalidFieldDescriptor);
    }

    @Test
    public void shouldThrowExceptionWhenNestedColumnDoesNotExists() {
        Descriptors.Descriptor parentDescriptor = TestEnrichedBookingLogMessage.getDescriptor();
        String invalidNestedFieldNames = "booking_log.driver_pickup-location.name";
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> DescriptorsUtil.getFieldDescriptor(parentDescriptor, invalidNestedFieldNames));
        assertEquals("Field Descriptor not found for field: driver_pickup-location in the proto of com.gotocompany.dagger.consumer.TestBookingLogMessage",
                exception.getMessage());
    }
    @Test
    public void shouldThrowExceptionWhenNestedColumnGetFieldDescriptorDoesNotExists() {
        Descriptors.Descriptor parentDescriptor = TestEnrichedBookingLogMessage.getDescriptor();
        String[] invalidNestedFieldNames = {"booking_log", "driver_pickup-location", "name"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> DescriptorsUtil.getNestedFieldDescriptor(parentDescriptor, invalidNestedFieldNames));
        assertEquals("Field Descriptor not found for field: driver_pickup-location in the proto of com.gotocompany.dagger.consumer.TestBookingLogMessage",
                exception.getMessage());
    }
}
