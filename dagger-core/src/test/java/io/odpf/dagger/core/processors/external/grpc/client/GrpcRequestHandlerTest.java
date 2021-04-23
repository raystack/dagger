package io.odpf.dagger.core.processors.external.grpc.client;

import io.odpf.dagger.consumer.TestGrpcRequest;
import io.odpf.dagger.core.exception.InvalidGrpcBodyException;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.external.grpc.GrpcSourceConfig;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcRequestHandlerTest {

    private GrpcSourceConfig grpcSourceConfig;

    private DescriptorManager descriptorManager;

    private Object[] requestVariablesValues;

    @Test
    public void bodyShouldBeCreatedForProperPayload() throws InvalidProtocolBufferException {

        grpcSourceConfig = mock(GrpcSourceConfig.class);
        descriptorManager = mock(DescriptorManager.class);
        TestGrpcRequest.newBuilder().setField1("val1").setField2("val2").build();

        requestVariablesValues = new Object[]{"val1", "val2"};

        when(grpcSourceConfig.getGrpcRequestProtoSchema()).thenReturn("TestGrpcRequest");
        when(grpcSourceConfig.getPattern()).thenReturn("{'field1': '%s' , 'field2' : '%s'}");
        when(descriptorManager.getDescriptor(any())).thenReturn(TestGrpcRequest.getDescriptor());

        GrpcRequestHandler grpcRequestHandler = new GrpcRequestHandler(grpcSourceConfig, descriptorManager);


        DynamicMessage message = grpcRequestHandler.create(requestVariablesValues);

        Descriptors.FieldDescriptor field1 = TestGrpcRequest.getDescriptor().findFieldByName("field1");
        Descriptors.FieldDescriptor field2 = TestGrpcRequest.getDescriptor().findFieldByName("field2");
        Assert.assertEquals("val1", message.getField(field1));
        Assert.assertEquals("val2", message.getField(field2));

    }

    @Test(expected = InvalidGrpcBodyException.class)
    public void shouldThrowExceptionInCaseOfWrongBody() throws InvalidProtocolBufferException {

        grpcSourceConfig = mock(GrpcSourceConfig.class);
        descriptorManager = mock(DescriptorManager.class);
        TestGrpcRequest.newBuilder().setField1("val1").setField2("val2").build();

        requestVariablesValues = new Object[]{"val1", "val2"};

        when(grpcSourceConfig.getGrpcRequestProtoSchema()).thenReturn("TestGrpcRequest");
        when(grpcSourceConfig.getPattern()).thenReturn("{'field1': '%s' , 'field2' : '%s'");
        when(descriptorManager.getDescriptor(any())).thenReturn(TestGrpcRequest.getDescriptor());

        GrpcRequestHandler grpcRequestHandler = new GrpcRequestHandler(grpcSourceConfig, descriptorManager);

        grpcRequestHandler.create(requestVariablesValues);

    }

    @Test(expected = InvalidGrpcBodyException.class)
    public void shouldThrowExceptionInCaseOfEmptyPattern() {
        grpcSourceConfig = mock(GrpcSourceConfig.class);
        descriptorManager = mock(DescriptorManager.class);
        TestGrpcRequest.newBuilder().setField1("val1").setField2("val2").build();

        requestVariablesValues = new Object[]{"val1", "val2"};

        when(grpcSourceConfig.getGrpcRequestProtoSchema()).thenReturn("TestGrpcRequest");
        when(grpcSourceConfig.getPattern()).thenReturn("");
        when(descriptorManager.getDescriptor(any())).thenReturn(TestGrpcRequest.getDescriptor());

        GrpcRequestHandler grpcRequestHandler = new GrpcRequestHandler(grpcSourceConfig, descriptorManager);
        grpcRequestHandler.create(requestVariablesValues);
    }
}
