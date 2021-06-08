package io.odpf.dagger.core.protohandler;

import io.odpf.dagger.consumer.TestBookingLogKey;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.junit.Assert.assertEquals;

public class TypeInformationFactoryTest {

    @Test
    public void shouldReturnTypeInformationForDescriptor() {
        Descriptors.Descriptor descriptor = TestBookingLogKey.getDescriptor();
        TypeInformation<Row> actualTypeInformation = TypeInformationFactory.getRowType(descriptor);
        TypeInformation<Row> expectedTypeInformation = ROW_NAMED(new String[]{"service_type", "order_number", "order_url",
                "status", "event_timestamp"}, STRING, STRING, STRING, STRING, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test(expected = DescriptorNotFoundException.class)
    public void shouldThrowExceptionIfNullPassed() {
        TypeInformationFactory.getRowType(null);
    }

}
