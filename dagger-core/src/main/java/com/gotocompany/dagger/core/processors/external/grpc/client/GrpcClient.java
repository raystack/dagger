package com.gotocompany.dagger.core.processors.external.grpc.client;

import com.gotocompany.dagger.core.exception.ChannelNotAvailableException;
import com.gotocompany.dagger.core.processors.external.grpc.GrpcSourceConfig;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The Grpc client.
 */
public class GrpcClient {
    private final GrpcSourceConfig grpcConfig;

    private ManagedChannel decoratedChannel;

    /**
     * Instantiates a new Grpc client.
     *
     * @param grpcConfig the grpc config
     */
    public GrpcClient(GrpcSourceConfig grpcConfig) {
        this.grpcConfig = grpcConfig;
    }

    /**
     * Add channel.
     */
    public void addChannel() {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(grpcConfig.getEndpoint(), grpcConfig.getServicePort()).usePlaintext();
        channelBuilder = decorateManagedChannelBuilder(channelBuilder);
        decoratedChannel = channelBuilder.build();
    }

    protected  ManagedChannelBuilder<?> decorateManagedChannelBuilder(ManagedChannelBuilder<?> channelBuilder) {
        if (StringUtils.isNotEmpty(grpcConfig.getGrpcArgKeepaliveTimeMs())) {
            channelBuilder = channelBuilder.keepAliveTime(Long.parseLong(grpcConfig.getGrpcArgKeepaliveTimeMs()), TimeUnit.MILLISECONDS);
        }
        if (StringUtils.isNotEmpty(grpcConfig.getGrpcArgKeepaliveTimeoutMs())) {
            channelBuilder = channelBuilder.keepAliveTimeout(Long.parseLong(grpcConfig.getGrpcArgKeepaliveTimeoutMs()), TimeUnit.MILLISECONDS);
        }
        if (grpcConfig.getHeaders() != null && !grpcConfig.getHeaders().isEmpty()) {
            Metadata metadata = new Metadata();
            for (Map.Entry<String, String> header : grpcConfig.getHeaders().entrySet()) {
                metadata.put(Metadata.Key.of(header.getKey(), Metadata.ASCII_STRING_MARSHALLER), header.getValue());
            }
            channelBuilder.intercept(MetadataUtils.newAttachHeadersInterceptor(metadata));
        }

        return channelBuilder;
    }

    /**
     * Async unary call.
     *
     * @param request          the request
     * @param responseObserver the response observer
     * @param inputDescriptor  the input descriptor
     * @param outputDescriptor the output descriptor
     * @throws Exception the exception
     */
    public void asyncUnaryCall(
            DynamicMessage request,
            StreamObserver<DynamicMessage> responseObserver, Descriptor inputDescriptor, Descriptor outputDescriptor) throws Exception {

        if (decoratedChannel == null) {
            throw new ChannelNotAvailableException("channel not available");
        }

        ClientCalls.asyncUnaryCall(
                createCall(CallOptions.DEFAULT, inputDescriptor, outputDescriptor),
                request,
                responseObserver);
    }

    private ClientCall<DynamicMessage, DynamicMessage> createCall(CallOptions callOptions, Descriptor inputDescriptor, Descriptor outputDescriptor) {

        return decoratedChannel.newCall(MethodDescriptor.newBuilder(new DynamicMessageMarshaller(inputDescriptor), new DynamicMessageMarshaller(outputDescriptor))
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(grpcConfig.getGrpcMethodUrl())
                .build(), callOptions);
    }

    /**
     * Close channel.
     */
    public void close() {
        if (decoratedChannel != null && decoratedChannel.isShutdown()) {
            decoratedChannel.shutdown();
        }
        this.decoratedChannel = null;
    }

    /**
     * Gets decorated channel.
     *
     * @return the decorated channel
     */
    public Channel getDecoratedChannel() {
        return decoratedChannel;
    }
}

