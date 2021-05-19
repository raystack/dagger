package io.odpf.dagger.functions.transformers.hash;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.functions.exceptions.InvalidHashFieldException;
import io.odpf.dagger.functions.transformers.hash.field.RowHasher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.MockitoAnnotations.initMocks;

public class PathReaderTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateRowHasherMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("order_number");
        fieldsToEncrypt.add("cancel_reason_id");
        fieldsToEncrypt.add("booking_creation_time.nanos");

        List<String> inputColumns = Arrays.asList("order_number", "cancel_reason_id", "booking_creation_time", "status");
        PathReader pathReader = new PathReader(descriptor, inputColumns);

        Map<String, RowHasher> rowHasherMap = pathReader.fieldMaskingPath(fieldsToEncrypt);

        Assert.assertEquals(fieldsToEncrypt.size(), rowHasherMap.size());
        Assert.assertTrue(rowHasherMap.get("order_number") instanceof RowHasher);
    }

    @Test
    public void shouldThrowErrorIfUnableToCreateHasherMap() {
        thrown.expect(InvalidHashFieldException.class);
        thrown.expectMessage("No primitive field found for hashing");
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("order_number");
        fieldsToEncrypt.add("cancel_reason_id");
        fieldsToEncrypt.add("booking_creation_time.random");

        List<String> inputColumns = Arrays.asList("order_number", "cancel_reason_id", "booking_creation_time", "status");
        PathReader pathReader = new PathReader(descriptor, inputColumns);

        pathReader.fieldMaskingPath(fieldsToEncrypt);
    }
}
