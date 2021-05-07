package io.odpf.dagger.core.processors.external.grpc.client;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestGrpcRequest;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DynamicMessageMarshallerTest {

    @Test
    public void testInputStream() throws IOException {
        DynamicMessageMarshaller dynamicMessageMarshaller = new DynamicMessageMarshaller(TestGrpcRequest.getDescriptor());

        TestGrpcRequest request = TestGrpcRequest.newBuilder().setField1("yes").build();

        DynamicMessage message = DynamicMessage.parseFrom(TestGrpcRequest.getDescriptor(), request.toByteArray());

        InputStream stream = dynamicMessageMarshaller.stream(message);

        Assert.assertTrue(IOUtils.contentEquals(stream, message.toByteString().newInput()));

    }

    @Test(expected = RuntimeException.class)
    public void parseShouldFailForWrongStream() throws InvalidProtocolBufferException {

        DynamicMessageMarshaller dynamicMessageMarshaller = new DynamicMessageMarshaller(TestGrpcRequest.getDescriptor());

        String random = "sdaskndkjsankjas";
        InputStream targetStream = new ByteArrayInputStream(random.getBytes());

        dynamicMessageMarshaller.parse(targetStream);

    }
}
