package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestEnrichedBookingLogMessage;
import io.odpf.dagger.functions.exceptions.RowHashException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.MockitoAnnotations.initMocks;

public class StringFieldHasherTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessStringLeafRow() {
        Descriptors.Descriptor parentDescriptor = TestBookingLogMessage.getDescriptor();
        String[] fieldPath = {"order_number"};
        StringFieldHasher stringFieldHasher = new StringFieldHasher(fieldPath);

        Assert.assertTrue(stringFieldHasher.canProcess(parentDescriptor.findFieldByName("order_number")));
    }

    @Test
    public void shouldNotProcessRepeatedStringLeafRow() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("meta_array");
        String[] fieldPath = {"meta_array"};
        StringFieldHasher stringFieldHasher = new StringFieldHasher(fieldPath);

        Assert.assertFalse(stringFieldHasher.canProcess(fieldDescriptor));
    }

    @Test
    public void shouldNotProcessNonLeafRow() {
        Descriptors.Descriptor bookingDescriptor = TestEnrichedBookingLogMessage.getDescriptor();
        String[] fieldPath = {"booking_log", "order_number"};
        StringFieldHasher stringFieldHasher = new StringFieldHasher(fieldPath);

        Assert.assertFalse(stringFieldHasher.canProcess(bookingDescriptor.findFieldByName("order_number")));
    }

    @Test
    public void shouldNotSetAnyChild() {
        Descriptors.Descriptor bookingDescriptor = TestBookingLogMessage.getDescriptor();
        String[] fieldPath = {"order_number"};
        StringFieldHasher stringFieldHasher = new StringFieldHasher(fieldPath);
        FieldHasher fieldHasher = stringFieldHasher.setChild(bookingDescriptor.findFieldByName("order_number"));

        Assert.assertEquals(stringFieldHasher, fieldHasher);
    }

    @Test
    public void shouldMaskStringFieldInARow() {
        String[] fieldPath = {"order_number"};
        StringFieldHasher stringFieldHasher = new StringFieldHasher(fieldPath);

        Assert.assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", stringFieldHasher.maskRow("hello"));
    }

    @Test
    public void shouldThrowExceptionIfNotableToHash() {
        thrown.expect(RowHashException.class);
        thrown.expectMessage("Unable to hash String value for field : order_number");
        String[] fieldPath = {"order_number"};
        StringFieldHasher stringFieldHasher = new StringFieldHasher(fieldPath);
        stringFieldHasher.maskRow(1);
    }

    @Test
    public void shouldCheckValidityOfField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("meta_array");
        String[] fieldPath = {"meta_array"};
        StringFieldHasher stringFieldHasher = new StringFieldHasher(fieldPath);

        Assert.assertEquals(false, stringFieldHasher.isValidNonRepeatedField(fieldDescriptor));
        Assert.assertEquals(false, stringFieldHasher.isValidNonRepeatedField(null));
    }
}
