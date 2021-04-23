package io.odpf.dagger.core.processors.external.grpc.client;

import io.odpf.dagger.core.exception.InvalidGrpcBodyException;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.external.grpc.GrpcSourceConfig;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;

public class GrpcRequestHandler {

    private GrpcSourceConfig grpcSourceConfig;
    private DescriptorManager descriptorManager;

    public GrpcRequestHandler(GrpcSourceConfig grpcSourceConfig, DescriptorManager descriptorManager) {
        this.grpcSourceConfig = grpcSourceConfig;
        this.descriptorManager = descriptorManager;
    }

    public DynamicMessage create(Object[] requestVariablesValues) {
        String requestBody = String.format(grpcSourceConfig.getPattern(), requestVariablesValues).replaceAll("'", "\"");

        try {

            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema()));
            JsonFormat.parser().merge(requestBody, builder);

            return builder.build();

        } catch (Exception e) {
            throw new InvalidGrpcBodyException(e.getMessage());
        }

    }

}
