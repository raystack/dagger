package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;

import java.io.Serializable;
import java.util.List;

/**
 * The Descriptor manager.
 */
public class DescriptorManager implements Serializable {
    private StencilClient stencilClient;

    /**
     * Instantiates a new Descriptor manager.
     *
     * @param stencilClientOrchestrator the stencil client orchestrator
     */
    public DescriptorManager(StencilClientOrchestrator stencilClientOrchestrator) {
        stencilClient = stencilClientOrchestrator.getStencilClient();
    }

    /**
     * Instantiates a new Descriptor manager with specified stencil urls.
     *
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param additionalStencilUrls     the additional stencil urls
     */
    public DescriptorManager(StencilClientOrchestrator stencilClientOrchestrator, List<String> additionalStencilUrls) {
        stencilClient = stencilClientOrchestrator.enrichStencilClient(additionalStencilUrls);
    }

    /**
     * Gets descriptor.
     *
     * @param protoClassName the proto class name
     * @return the descriptor
     */
    public Descriptors.Descriptor getDescriptor(String protoClassName) {
        Descriptors.Descriptor descriptor = stencilClient.get(protoClassName);
        if (descriptor == null) {
            throw new DescriptorNotFoundException("No Descriptor found for class "
                    + protoClassName);
        }
        return descriptor;
    }
}
