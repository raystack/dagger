package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.stencil.StencilClientFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FiltersTest {
    @Mock
    private Predicate<DynamicMessage> alwaysTrue;
    @Mock
    private Predicate<DynamicMessage> alwaysFalse;
    @Mock
    private Predicate<DynamicMessage> predicate;

    private final FieldDescriptor orderNumberFieldDesc = TestBookingLogMessage.getDescriptor()
            .findFieldByName("order_number");

    @Before
    public void setup() {
        initMocks(this);
        when(alwaysTrue.test(any())).thenReturn(true);
    }

    @Test
    public void shouldReturnEverythingFromInputWhenPredicateIsTrue()
            throws ClassNotFoundException, InvalidProtocolBufferException {
        ByteString inputBytes1 = TestBookingLogMessage.newBuilder().setOrderNumber("order_1").build().toByteString();
        ByteString inputBytes2 = TestBookingLogMessage.newBuilder().setOrderNumber("order_2").build().toByteString();
        ByteString[] inputBytes = new ByteString[]{inputBytes1, inputBytes2};

        Filters filter = new Filters(StencilClientFactory.getClient());
        List<DynamicMessage> result = filter.eval(inputBytes, "io.odpf.dagger.consumer.TestBookingLogMessage", alwaysTrue);

        assertEquals("order_1", result.get(0).getField(orderNumberFieldDesc));
        assertEquals("order_2", result.get(1).getField(orderNumberFieldDesc));
    }

    @Test
    public void shouldReturnNothingFromInputWhenPredicateIsFalse()
            throws ClassNotFoundException, InvalidProtocolBufferException {
        ByteString inputBytes1 = TestBookingLogMessage.newBuilder().setOrderNumber("order_1").build().toByteString();
        ByteString inputBytes2 = TestBookingLogMessage.newBuilder().setOrderNumber("order_2").build().toByteString();
        ByteString[] inputBytes = new ByteString[]{inputBytes1, inputBytes2};

        Filters filter = new Filters(StencilClientFactory.getClient());
        List<DynamicMessage> result = filter.eval(inputBytes, "io.odpf.dagger.consumer.TestBookingLogMessage", alwaysFalse);

        assertEquals(0, result.size());
    }

    @Test
    public void shouldReturnOnlyWhenPredicateIsTrue()
            throws ClassNotFoundException, InvalidProtocolBufferException {
        when(predicate.test(any())).thenReturn(true).thenReturn(false);

        ByteString inputBytes1 = TestBookingLogMessage.newBuilder().setOrderNumber("order_1").build().toByteString();
        ByteString inputBytes2 = TestBookingLogMessage.newBuilder().setOrderNumber("order_2").build().toByteString();
        ByteString[] inputBytes = new ByteString[]{inputBytes1, inputBytes2};

        Filters filter = new Filters(StencilClientFactory.getClient());
        List<DynamicMessage> result = filter.eval(inputBytes, "io.odpf.dagger.consumer.TestBookingLogMessage", predicate);

        assertEquals(1, result.size());
        assertEquals("order_1", result.get(0).getField(orderNumberFieldDesc));
    }

    @Test
    public void shouldAcceptTwoPredicates() throws ClassNotFoundException, InvalidProtocolBufferException {
        when(predicate.test(any())).thenReturn(true).thenReturn(false);

        ByteString inputBytes1 = TestBookingLogMessage.newBuilder().setOrderNumber("order_1").build().toByteString();
        ByteString inputBytes2 = TestBookingLogMessage.newBuilder().setOrderNumber("order_2").build().toByteString();
        ByteString[] inputBytes = new ByteString[]{inputBytes1, inputBytes2};

        Filters filter = new Filters(StencilClientFactory.getClient());
        List<DynamicMessage> result = filter.eval(inputBytes, "io.odpf.dagger.consumer.TestBookingLogMessage", alwaysTrue, predicate);

        assertEquals(1, result.size());
        assertEquals("order_1", result.get(0).getField(orderNumberFieldDesc));
    }

    @Test
    public void shouldNotTestAgainstNextPredicateWhenFailEarly() throws ClassNotFoundException, InvalidProtocolBufferException {
        when(predicate.test(any())).thenReturn(true).thenReturn(false);

        ByteString inputBytes1 = TestBookingLogMessage.newBuilder().setOrderNumber("order_1").build().toByteString();
        ByteString inputBytes2 = TestBookingLogMessage.newBuilder().setOrderNumber("order_2").build().toByteString();
        ByteString[] inputBytes = new ByteString[]{inputBytes1, inputBytes2};

        Filters filter = new Filters(StencilClientFactory.getClient());
        List<DynamicMessage> result = filter.eval(inputBytes, "io.odpf.dagger.consumer.TestBookingLogMessage", alwaysTrue, predicate, alwaysTrue);

        assertEquals(1, result.size());
        assertEquals("order_1", result.get(0).getField(orderNumberFieldDesc));
        verify(alwaysTrue, times(3)).test(any());
    }
}
