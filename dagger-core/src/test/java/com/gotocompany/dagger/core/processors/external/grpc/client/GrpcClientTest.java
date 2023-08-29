package com.gotocompany.dagger.core.processors.external.grpc.client;

import com.gotocompany.dagger.common.exceptions.DescriptorNotFoundException;
import com.gotocompany.dagger.core.processors.common.OutputMapping;
import com.gotocompany.dagger.core.processors.external.grpc.GrpcSourceConfigBuilder;
import io.grpc.Channel;
import com.gotocompany.dagger.core.processors.external.grpc.GrpcSourceConfig;
import io.grpc.ManagedChannelBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.MockitoAnnotations.initMocks;

public class GrpcClientTest {

    @Mock
    private GrpcSourceConfig grpcSourceConfig;
    @Mock
    ManagedChannelBuilder channelBuilder;

    @Before
    public void setUp() {
        initMocks(this);
        when(grpcSourceConfig.getEndpoint()).thenReturn("localhost");
        when(grpcSourceConfig.getServicePort()).thenReturn(8080);
    }

    @Test
    public void channelShouldBeAddedForAHostAndPort() {

        GrpcClient grpcClient = new GrpcClient(grpcSourceConfig);

        grpcClient.addChannel();

        Channel decoratedChannel = grpcClient.getDecoratedChannel();
        assertNotNull(decoratedChannel);

    }

    @Test
    public void channelBuilderShouldNotBeDecoratedWithKeepaliveORTimeoutMS() {
        GrpcClient grpcClient = new GrpcClient(grpcSourceConfig);
        grpcClient.decorateManagedChannelBuilder(channelBuilder);
        verify(channelBuilder, never()).keepAliveTime(anyLong(), any());
        verify(channelBuilder, never()).keepAliveTimeout(anyLong(), any());
    }

    @Test
    public void channelBuilderShouldBeDecoratedWithKeepaliveMS() {
        when(grpcSourceConfig.getGrpcArgKeepaliveTimeMs()).thenReturn("1000");

        GrpcClient grpcClient = new GrpcClient(grpcSourceConfig);
        grpcClient.decorateManagedChannelBuilder(channelBuilder);
        verify(channelBuilder, times(1)).keepAliveTime(Long.parseLong("1000"),TimeUnit.MILLISECONDS);
        verify(channelBuilder, never()).keepAliveTimeout(anyLong(), any());
    }

    @Test
    public void channelBuilderShouldBeDecoratedWithKeepaliveAndTimeOutMS() {
        when(grpcSourceConfig.getGrpcArgKeepaliveTimeMs()).thenReturn("1000");
        when(grpcSourceConfig.getGrpcArgKeepaliveTimeoutMs()).thenReturn("100");
        when(channelBuilder.keepAliveTime(anyLong(),any())).thenReturn(channelBuilder);

        GrpcClient grpcClient = new GrpcClient(grpcSourceConfig);
        grpcClient.decorateManagedChannelBuilder(channelBuilder);
        verify(channelBuilder, times(1)).keepAliveTimeout(Long.parseLong("100"),TimeUnit.MILLISECONDS);
        verify(channelBuilder, times(1)).keepAliveTime(Long.parseLong("1000"), TimeUnit.MILLISECONDS);
    }

    @Test
    public void grpcClientCloseShouldWork() {

        GrpcClient grpcClient = new GrpcClient(grpcSourceConfig);

        grpcClient.addChannel();

        Channel decoratedChannel = grpcClient.getDecoratedChannel();
        assertNotNull(decoratedChannel);

        grpcClient.close();
        decoratedChannel = grpcClient.getDecoratedChannel();
        assertNull(decoratedChannel);

    }

}
