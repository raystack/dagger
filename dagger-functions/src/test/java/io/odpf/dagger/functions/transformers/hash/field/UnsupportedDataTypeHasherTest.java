package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import io.odpf.dagger.consumer.TestMessage;
import io.odpf.dagger.functions.exceptions.InvalidHashFieldException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.MockitoAnnotations.initMocks;

public class UnsupportedDataTypeHasherTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessNothing() {
        Descriptors.Descriptor timeStampDescriptor = Timestamp.getDescriptor();
        String[] fieldPath = {"seconds"};
        UnsupportedDataTypeHasher unsupportedDataTypeHasher = new UnsupportedDataTypeHasher(fieldPath);

        Assert.assertFalse(unsupportedDataTypeHasher.canProcess(timeStampDescriptor.findFieldByName("seconds")));
    }

    @Test
    public void shouldNotMaskAnything() {
        String[] fieldPath = {"seconds"};
        UnsupportedDataTypeHasher unsupportedDataTypeHasher = new UnsupportedDataTypeHasher(fieldPath);
        Assert.assertEquals("test_field", unsupportedDataTypeHasher.maskRow("test_field"));
    }

    @Test
    public void shouldThrowExceptionWhileSettingChild() {
        thrown.expect(InvalidHashFieldException.class);
        String[] fieldPath = {"seconds"};
        UnsupportedDataTypeHasher unsupportedDataTypeHasher = new UnsupportedDataTypeHasher(fieldPath);

        unsupportedDataTypeHasher.setChild(null);
    }

    @Test
    public void shouldThrowExceptionForInvalidDataType() {
        thrown.expect(InvalidHashFieldException.class);
        Descriptors.FieldDescriptor fieldDescriptor = TestMessage
                .newBuilder()
                .getDescriptorForType()
                .findFieldByName("repeated_long_field");

        String[] fieldPath = {"repeated_long_field"};
        UnsupportedDataTypeHasher unsupportedDataTypeHasher = new UnsupportedDataTypeHasher(fieldPath);

        unsupportedDataTypeHasher.setChild(fieldDescriptor);
    }
}
