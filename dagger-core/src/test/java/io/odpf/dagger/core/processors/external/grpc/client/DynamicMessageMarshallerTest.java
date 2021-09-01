package io.odpf.dagger.core.processors.external.grpc.client;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestGrpcRequest;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;

public class DynamicMessageMarshallerTest {

    @Test
    public void testInputStream() throws IOException {
        DynamicMessageMarshaller dynamicMessageMarshaller = new DynamicMessageMarshaller(TestGrpcRequest.getDescriptor());

        TestGrpcRequest request = TestGrpcRequest.newBuilder().setField1("yes").build();

        DynamicMessage message = DynamicMessage.parseFrom(TestGrpcRequest.getDescriptor(), request.toByteArray());

        InputStream stream = dynamicMessageMarshaller.stream(message);

        assertTrue(IOUtils.contentEquals(stream, message.toByteString().newInput()));

    }

    @Test
    public void parseShouldFailForWrongStream() throws InvalidProtocolBufferException {

        DynamicMessageMarshaller dynamicMessageMarshaller = new DynamicMessageMarshaller(TestGrpcRequest.getDescriptor());

        String random = "sdaskndkjsankjas";
        InputStream targetStream = new ByteArrayInputStream(random.getBytes());

        RuntimeException runtimeException = assertThrows(RuntimeException.class,
                () -> dynamicMessageMarshaller.parse(targetStream));
        assertEquals("Unable to merge from the supplied input stream", runtimeException.getMessage());

    }
}
