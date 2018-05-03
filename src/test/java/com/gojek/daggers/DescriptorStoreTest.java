package com.gojek.daggers;

import com.google.protobuf.Descriptors;
import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DescriptorStoreTest {

    @Before
    public void before() {
        DescriptorStore.load(new Configuration());
    }

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
