package io.odpf.dagger.core.processors.external.grpc.client;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestGrpcRequest;
import io.odpf.dagger.core.exception.InvalidGrpcBodyException;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.external.grpc.GrpcSourceConfig;
import io.odpf.dagger.core.processors.external.grpc.GrpcSourceConfigBuilder;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcRequestHandlerTest {

    private GrpcSourceConfig grpcSourceConfig;

    private DescriptorManager descriptorManager;

    private Object[] requestVariablesValues;

    @Test
    public void bodyShouldBeCreatedForProperPayload() throws InvalidProtocolBufferException {
        grpcSourceConfig = new GrpcSourceConfigBuilder()
                .setGrpcRequestProtoSchema("TestGrpcRequest")
                .setRequestPattern("{'field1': '%s' , 'field2' : '%s'}")
                .createGrpcSourceConfig();
        descriptorManager = mock(DescriptorManager.class);
        TestGrpcRequest.newBuilder().setField1("val1").setField2("val2").build();

        requestVariablesValues = new Object[]{"val1", "val2"};

        when(descriptorManager.getDescriptor(any())).thenReturn(TestGrpcRequest.getDescriptor());

        GrpcRequestHandler grpcRequestHandler = new GrpcRequestHandler(grpcSourceConfig, descriptorManager);


        DynamicMessage message = grpcRequestHandler.create(requestVariablesValues);

        Descriptors.FieldDescriptor field1 = TestGrpcRequest.getDescriptor().findFieldByName("field1");
        Descriptors.FieldDescriptor field2 = TestGrpcRequest.getDescriptor().findFieldByName("field2");
        assertEquals("val1", message.getField(field1));
        assertEquals("val2", message.getField(field2));

    }

    @Test
    public void shouldThrowExceptionInCaseOfWrongBody() throws InvalidProtocolBufferException {
        grpcSourceConfig = new GrpcSourceConfigBuilder()
                .setGrpcRequestProtoSchema("TestGrpcRequest")
                .setRequestPattern("{'field1': '%s' , 'field2' : '%s'")
                .createGrpcSourceConfig();
        descriptorManager = mock(DescriptorManager.class);
        TestGrpcRequest.newBuilder().setField1("val1").setField2("val2").build();

        requestVariablesValues = new Object[]{"val1", "val2"};

        when(descriptorManager.getDescriptor(any())).thenReturn(TestGrpcRequest.getDescriptor());

        GrpcRequestHandler grpcRequestHandler = new GrpcRequestHandler(grpcSourceConfig, descriptorManager);

        assertThrows(InvalidGrpcBodyException.class, () -> grpcRequestHandler.create(requestVariablesValues));

    }

    @Test
    public void shouldThrowExceptionInCaseOfEmptyPattern() {
        grpcSourceConfig = new GrpcSourceConfigBuilder()
                .setGrpcRequestProtoSchema("TestGrpcRequest")
                .setRequestPattern("")
                .createGrpcSourceConfig();

        descriptorManager = mock(DescriptorManager.class);
        TestGrpcRequest.newBuilder().setField1("val1").setField2("val2").build();

        requestVariablesValues = new Object[]{"val1", "val2"};
        when(descriptorManager.getDescriptor(any())).thenReturn(TestGrpcRequest.getDescriptor());

        GrpcRequestHandler grpcRequestHandler = new GrpcRequestHandler(grpcSourceConfig, descriptorManager);
        assertThrows(InvalidGrpcBodyException.class, () -> grpcRequestHandler.create(requestVariablesValues));
    }
}
