package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.stencil.client.StencilClient;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

public class ToJSON extends ScalarUdf {
    private StencilClientOrchestrator stencilClientOrchestrator;
    private StencilClient stencilClient;

    /**
     * Instantiates a new ToJSON.
     *
     * @param stencilClientOrchestrator the stencil client orchestrator
     */
    public ToJSON(StencilClientOrchestrator stencilClientOrchestrator) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
    }

    /**
     * Instantiates a new ToJSON.
     *
     * @param stencilClient the stencil client
     */
    public ToJSON(StencilClient stencilClient) {
        this.stencilClient = stencilClient;
    }

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object inputProtoBytes, String protoClassName) throws ClassNotFoundException, InvalidProtocolBufferException {
        if (!(inputProtoBytes instanceof ByteString)) {
            return null;
        }
        ByteString inputData = (ByteString) inputProtoBytes;
        Descriptors.Descriptor descriptor = getDescriptor(protoClassName);
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, inputData);
        return JsonFormat.printer().includingDefaultValueFields().preservingProtoFieldNames().print(dynamicMessage);
    }

    private Descriptors.Descriptor getDescriptor(String protoClassName) throws ClassNotFoundException {
        Descriptors.Descriptor descriptor = stencilClient.get(protoClassName);
        if (descriptor == null) {
            throw new ClassNotFoundException(protoClassName);
        }
        return descriptor;
    }

    /**
     * Gets stencil client.
     *
     * @return the stencil client
     */
    public StencilClient getStencilClient() {
        if (stencilClient != null) {
            return stencilClient;
        }
        return stencilClientOrchestrator.getStencilClient();
    }
}
