package org.raystack.dagger.common.core;

import org.raystack.dagger.consumer.TestApiLogMessage;
import org.raystack.dagger.consumer.TestBookingLogMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FieldDescriptorCacheTest {


    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnTrueIfFieldPresentInMap() {
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());
        assertTrue(fieldDescriptorCache.containsField("org.raystack.dagger.consumer.TestBookingLogMessage.order_number"));
    }

    @Test
    public void shouldReturnFalseIfFieldNotPresentInMap() {
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());
        assertFalse(fieldDescriptorCache.containsField("xyz"));
    }

    @Test
    public void shouldReturnOriginalFieldIndex() {
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());
        assertEquals(1, fieldDescriptorCache.getOriginalFieldIndex(TestBookingLogMessage.getDescriptor().findFieldByName("order_number")));
    }

    @Test
    public void shouldReturnOriginalFieldCount() {
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());
        assertEquals(49, fieldDescriptorCache.getOriginalFieldCount(TestBookingLogMessage.getDescriptor()));
    }

    @Test
    public void shouldThrowExceptionIfFieldNotPresentInCacheForFieldCount() {
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());
        assertThrows("The Proto Descriptor org.raystack.dagger.consumer.TestApiLogMessage was not found in the cache", IllegalArgumentException.class, () -> fieldDescriptorCache.getOriginalFieldCount(TestApiLogMessage.getDescriptor()));
    }

    @Test
    public void shouldThrowExceptionIfFieldNotPresentInCacheForFieldIndex() {
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());
        assertThrows("The Field Descriptor org.raystack.dagger.consumer.TestApiLogMessage.event_timestamp was not found in the cache", IllegalArgumentException.class, () -> fieldDescriptorCache.getOriginalFieldIndex(TestApiLogMessage.getDescriptor().findFieldByName("event_timestamp")));
    }
}
