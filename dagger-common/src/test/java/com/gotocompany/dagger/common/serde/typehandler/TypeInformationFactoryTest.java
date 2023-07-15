package org.raystack.dagger.common.serde.typehandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import org.raystack.dagger.common.exceptions.DescriptorNotFoundException;
import org.raystack.dagger.consumer.TestBookingLogKey;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.api.common.typeinfo.Types.INT;
import static org.apache.flink.api.common.typeinfo.Types.LONG;
import static org.apache.flink.api.common.typeinfo.Types.ROW_NAMED;
import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.junit.Assert.assertEquals;

public class TypeInformationFactoryTest {

    @Test
    public void shouldReturnTypeInformationForDescriptor() {
        Descriptors.Descriptor descriptor = TestBookingLogKey.getDescriptor();
        TypeInformation<Row> actualTypeInformation = TypeInformationFactory.getRowType(descriptor);
        TypeInformation<Row> expectedTypeInformation = ROW_NAMED(new String[] {"service_type", "order_number", "order_url",
                "status", "event_timestamp"}, STRING, STRING, STRING, STRING, ROW_NAMED(new String[] {"seconds", "nanos"}, LONG, INT));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldThrowExceptionIfNullPassed() {
        Assert.assertThrows(DescriptorNotFoundException.class,
                () -> TypeInformationFactory.getRowType(null));

    }

}
