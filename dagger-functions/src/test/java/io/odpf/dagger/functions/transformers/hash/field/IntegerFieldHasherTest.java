package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.functions.exceptions.RowHashException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.MockitoAnnotations.initMocks;

public class IntegerFieldHasherTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessIntegerLeafRow() {
        Descriptors.Descriptor timeStampDescriptor = Timestamp.getDescriptor();
        String[] fieldPath = {"nanos"};
        IntegerFieldHasher integerFieldHasher = new IntegerFieldHasher(fieldPath);

        Assert.assertTrue(integerFieldHasher.canProcess(timeStampDescriptor.findFieldByName("nanos")));
    }

    @Test
    public void shouldNotProcessRepeatedIntegerLeafRow() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("int_array_field");
        String[] fieldPath = {"int_array_field"};
        IntegerFieldHasher integerFieldHasher = new IntegerFieldHasher(fieldPath);

        Assert.assertFalse(integerFieldHasher.canProcess(fieldDescriptor));
    }

    @Test
    public void shouldNotProcessNonLeafRow() {
        Descriptors.Descriptor timeStampDescriptor = Timestamp.getDescriptor();
        String[] fieldPath = {"created_at", "nanos"};
        IntegerFieldHasher integerFieldHasher = new IntegerFieldHasher(fieldPath);

        Assert.assertFalse(integerFieldHasher.canProcess(timeStampDescriptor.findFieldByName("nanos")));
    }

    @Test
    public void shouldNotSetAnyChild() {
        Descriptors.Descriptor timeStampDescriptor = Timestamp.getDescriptor();
        String[] fieldPath = {"nanos"};
        IntegerFieldHasher integerFieldHasher = new IntegerFieldHasher(fieldPath);
        FieldHasher fieldHasher = integerFieldHasher.setChild(timeStampDescriptor.findFieldByName("nanos"));

        Assert.assertEquals(integerFieldHasher, fieldHasher);
    }

    @Test
    public void shouldMaskIntegerFieldInARow() {
        String[] fieldPath = {"nanos"};
        IntegerFieldHasher integerFieldHasher = new IntegerFieldHasher(fieldPath);

        Assert.assertEquals(-1176347385, integerFieldHasher.maskRow(10));
    }

    @Test
    public void shouldThrowExceptionIfNotableToHash() {
        thrown.expect(RowHashException.class);
        thrown.expectMessage("Unable to hash int value for field : nanos");
        String[] fieldPath = {"nanos"};
        IntegerFieldHasher integerFieldHasher = new IntegerFieldHasher(fieldPath);
        integerFieldHasher.maskRow("test");
    }

    @Test
    public void shouldCheckValidityOfField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("int_array_field");
        String[] fieldPath = {"int_array_field"};
        IntegerFieldHasher integerFieldHasher = new IntegerFieldHasher(fieldPath);

        Assert.assertEquals(false, integerFieldHasher.isValidNonRepeatedField(fieldDescriptor));
        Assert.assertEquals(false, integerFieldHasher.isValidNonRepeatedField(null));
    }
}
