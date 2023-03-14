package com.gotocompany.dagger.functions.transformers.hash;

import com.google.protobuf.Descriptors;
import com.gotocompany.dagger.functions.exceptions.InvalidHashFieldException;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestEnrichedBookingLogMessage;
import com.gotocompany.dagger.functions.transformers.hash.field.FieldHasher;
import com.gotocompany.dagger.functions.transformers.hash.field.IntegerFieldHasher;
import com.gotocompany.dagger.functions.transformers.hash.field.LongFieldHasher;
import com.gotocompany.dagger.functions.transformers.hash.field.RowHasher;
import com.gotocompany.dagger.functions.transformers.hash.field.StringFieldHasher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.MockitoAnnotations.initMocks;

public class FieldHasherFactoryTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateChildForStringField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .getDescriptor()
                .findFieldByName("order_number");
        String[] fieldPath = {"order_number"};
        FieldHasher childHasher = new FieldHasherFactory().createChildHasher(fieldPath, fieldDescriptor);

        Assert.assertTrue(childHasher instanceof StringFieldHasher);
    }

    @Test
    public void shouldCreateChildForIntegerField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .getDescriptor()
                .findFieldByName("booking_creation_time")
                .getMessageType()
                .findFieldByName("nanos");
        String[] fieldPath = {"nanos"};
        FieldHasher childHasher = new FieldHasherFactory().createChildHasher(fieldPath, fieldDescriptor);

        Assert.assertTrue(childHasher instanceof IntegerFieldHasher);
    }

    @Test
    public void shouldCreateChildForLongField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage
                .getDescriptor()
                .findFieldByName("booking_creation_time")
                .getMessageType()
                .findFieldByName("seconds");
        String[] fieldPath = {"seconds"};
        FieldHasher childHasher = new FieldHasherFactory().createChildHasher(fieldPath, fieldDescriptor);

        Assert.assertTrue(childHasher instanceof LongFieldHasher);
    }

    @Test
    public void shouldCreateChildForRowField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestEnrichedBookingLogMessage
                .getDescriptor()
                .findFieldByName("booking_log");

        String[] fieldPath = {"booking_log", "order_number"};
        FieldHasher childHasher = new FieldHasherFactory().createChildHasher(fieldPath, fieldDescriptor);

        Assert.assertTrue(childHasher instanceof RowHasher);
    }

    @Test
    public void shouldThrowErrorForNonLeafField() {
        thrown.expect(InvalidHashFieldException.class);
        thrown.expectMessage("Inner Field : booking_log of data type : MESSAGE not currently supported for hashing");
        Descriptors.FieldDescriptor fieldDescriptor = TestEnrichedBookingLogMessage
                .getDescriptor()
                .findFieldByName("booking_log");

        String[] fieldPath = {"booking_log"};
        new FieldHasherFactory().createChildHasher(fieldPath, fieldDescriptor);
    }

    @Test
    public void shouldThrowErrorForNoFieldPresent() {
        thrown.expect(InvalidHashFieldException.class);
        thrown.expectMessage("No primitive field found for hashing");
        Descriptors.FieldDescriptor fieldDescriptor = TestEnrichedBookingLogMessage
                .getDescriptor()
                .findFieldByName("booking_log");

        String[] fieldPath = {};
        new FieldHasherFactory().createChildHasher(fieldPath, fieldDescriptor);
    }
}
