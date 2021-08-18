package io.odpf.dagger.core.processors.external.grpc.client;

import io.grpc.Channel;
import io.odpf.dagger.core.processors.external.grpc.GrpcSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcClientTest {

    private GrpcSourceConfig grpcSourceConfig;

    @Test
    public void channelShouldBeAddedForAHostAndPort() {

        grpcSourceConfig = mock(GrpcSourceConfig.class);

        GrpcClient grpcClient = new GrpcClient(grpcSourceConfig);

        when(grpcSourceConfig.getEndpoint()).thenReturn("localhost");
        when(grpcSourceConfig.getServicePort()).thenReturn(8080);

        grpcClient.addChannel();

        Channel decoratedChannel = grpcClient.getDecoratedChannel();
        //TODO use assert not null instead, use static imports
        Assert.assertTrue(decoratedChannel != null);

    }

    @Test
    public void grpcClientCloseShouldWork() {

        grpcSourceConfig = mock(GrpcSourceConfig.class);

        GrpcClient grpcClient = new GrpcClient(grpcSourceConfig);

        when(grpcSourceConfig.getEndpoint()).thenReturn("localhost");
        when(grpcSourceConfig.getServicePort()).thenReturn(8080);

        grpcClient.addChannel();

        Channel decoratedChannel = grpcClient.getDecoratedChannel();
        //TODO use assert not null instead, use static imports
        Assert.assertTrue(decoratedChannel != null);

        grpcClient.close();
        //TODO use assert null instead, use static imports
        Assert.assertTrue(grpcClient.getDecoratedChannel() == null);

    }

}
