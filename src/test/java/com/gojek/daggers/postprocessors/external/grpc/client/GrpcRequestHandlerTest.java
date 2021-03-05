package com.gojek.daggers.postprocessors.external.grpc.client;

import com.gojek.daggers.exception.InvalidGrpcBodyException;
import com.gojek.daggers.postprocessors.external.common.DescriptorManager;
import com.gojek.daggers.postprocessors.external.grpc.GrpcSourceConfig;
import com.gojek.esb.consumer.TestGrpcRequest;
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

        when(grpcSourceConfig.getGrpcRequestProtoSchema()).thenReturn("com.gojek.esb.consumer.TestGrpcRequest");
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

        when(grpcSourceConfig.getGrpcRequestProtoSchema()).thenReturn("com.gojek.esb.consumer.TestGrpcRequest");
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

        when(grpcSourceConfig.getGrpcRequestProtoSchema()).thenReturn("com.gojek.esb.consumer.TestGrpcRequest");
        when(grpcSourceConfig.getPattern()).thenReturn("");
        when(descriptorManager.getDescriptor(any())).thenReturn(TestGrpcRequest.getDescriptor());

        GrpcRequestHandler grpcRequestHandler = new GrpcRequestHandler(grpcSourceConfig, descriptorManager);
        grpcRequestHandler.create(requestVariablesValues);
    }
}
