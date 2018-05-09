package com.gojek.daggers;

import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.*;

public class DescriptorStoreTest {

    @Test
    public void get() {
        Descriptors.Descriptor dsc = DescriptorStore.get("com.gojek.esb.booking.BookingLogMessage");
        assertNotNull(dsc);
    }

    @Test(expected = DaggerConfigurationException.class)
    public void classNotPresent() {
        Descriptors.Descriptor dsc = DescriptorStore.get("com.gojek.esb.booking.BookingLogMessageNA");
    }
}