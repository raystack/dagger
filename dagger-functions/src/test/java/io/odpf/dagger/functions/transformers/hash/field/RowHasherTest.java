package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.MockitoAnnotations.initMocks;

public class RowHasherTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessRow() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("event_timestamp");

        String[] fieldPath = {"event_timestamp", "nanos"};
        RowHasher rowHasher = new RowHasher(fieldPath);

        Assert.assertTrue(rowHasher.canProcess(fieldDescriptor));
    }

    @Test
    public void shouldNotProcessRepeatedRow() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("routes");
        String[] fieldPath = {"routes[0"};
        RowHasher rowHasher = new RowHasher(fieldPath);

        Assert.assertFalse(rowHasher.canProcess(fieldDescriptor));
    }

    @Test
    public void shouldNotProcessLeafRow() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("order_number");
        String[] fieldPath = {"order_number"};
        RowHasher rowHasher = new RowHasher(fieldPath);

        Assert.assertFalse(rowHasher.canProcess(fieldDescriptor));
    }

    @Test
    public void shouldCheckValidityOfField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("routes");
        String[] fieldPath = {"routes"};
        RowHasher rowHasher = new RowHasher(fieldPath);

        Assert.assertFalse(rowHasher.isValidNonRepeatedField(fieldDescriptor));
        Assert.assertFalse(rowHasher.isValidNonRepeatedField(null));
    }
}
