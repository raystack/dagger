package io.odpf.dagger.protohandler;

import io.odpf.dagger.consumer.TestBookingLogKey;
import io.odpf.dagger.exception.DescriptorNotFoundException;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.junit.Assert.assertEquals;

public class TypeInformationFactoryTest {

    @Test
    public void shouldReturnTypeInformationForDescriptor() {
        Descriptors.Descriptor descriptor = TestBookingLogKey.getDescriptor();
        TypeInformation<Row> actualTypeInformation = TypeInformationFactory.getRowType(descriptor);
        TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(new String[]{"order_id", "status", "event_timestamp",
                "bid_id", "service_type", "participant_id", "audit"}, STRING, STRING, ROW_NAMED(new String[]{"seconds", "nanos"},
                LONG, INT), STRING, STRING, STRING, ROW_NAMED(new String[]{"request_id", "timestamp"}, STRING, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT)));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test(expected = DescriptorNotFoundException.class)
    public void shouldThrowExceptionIfNullPassed() {
        TypeInformationFactory.getRowType(null);
    }

}
