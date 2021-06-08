package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import io.odpf.dagger.consumer.TestMessage;
import io.odpf.dagger.functions.exceptions.RowHashException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.MockitoAnnotations.initMocks;

public class LongFieldHasherTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessLongLeafRow() {
        Descriptors.Descriptor timeStampDescriptor = Timestamp.getDescriptor();
        String[] fieldPath = {"seconds"};
        LongFieldHasher longFieldHasher = new LongFieldHasher(fieldPath);

        Assert.assertTrue(longFieldHasher.canProcess(timeStampDescriptor.findFieldByName("seconds")));
    }

    @Test
    public void shouldNotProcessRepeatedLongLeafRow() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("repeated_long_field");
        String[] fieldPath = {"repeated_long_field"};
        LongFieldHasher longFieldHasher = new LongFieldHasher(fieldPath);

        Assert.assertFalse(longFieldHasher.canProcess(fieldDescriptor));
    }

    @Test
    public void shouldNotProcessNonLeafRow() {
        Descriptors.Descriptor timeStampDescriptor = Timestamp.getDescriptor();
        String[] fieldPath = {"created_at", "seconds"};
        LongFieldHasher longFieldHasher = new LongFieldHasher(fieldPath);

        Assert.assertFalse(longFieldHasher.canProcess(timeStampDescriptor.findFieldByName("seconds")));
    }

    @Test
    public void shouldNotSetAnyChild() {
        Descriptors.Descriptor timeStampDescriptor = Timestamp.getDescriptor();
        String[] fieldPath = {"seconds"};
        LongFieldHasher longFieldHasher = new LongFieldHasher(fieldPath);
        FieldHasher fieldHasher = longFieldHasher.setChild(timeStampDescriptor.findFieldByName("seconds"));

        Assert.assertEquals(longFieldHasher, fieldHasher);
    }

    @Test
    public void shouldMaskLongFieldInARow() {
        String[] fieldPath = {"seconds"};
        LongFieldHasher longFieldHasher = new LongFieldHasher(fieldPath);

        Assert.assertEquals(-8613927256589200991L, longFieldHasher.maskRow(10L));
    }

    @Test
    public void shouldThrowExceptionIfNotableToHash() {
        thrown.expect(RowHashException.class);
        thrown.expectMessage("Unable to hash long value for field : seconds");
        String[] fieldPath = {"seconds"};
        LongFieldHasher longFieldHasher = new LongFieldHasher(fieldPath);
        longFieldHasher.maskRow("test");
    }

    @Test
    public void shouldCheckValidityOfField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("repeated_long_field");
        String[] fieldPath = {"repeated_long_field"};
        LongFieldHasher longFieldHasher = new LongFieldHasher(fieldPath);

        Assert.assertEquals(false, longFieldHasher.isValidNonRepeatedField(fieldDescriptor));
        Assert.assertEquals(false, longFieldHasher.isValidNonRepeatedField(null));
    }
}
