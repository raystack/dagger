package org.raystack.dagger.core.processors.common;

import com.google.protobuf.Descriptors;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.exceptions.DescriptorNotFoundException;
import org.raystack.dagger.consumer.TestBookingLogMessage;
import org.raystack.stencil.StencilClientFactory;
import org.raystack.stencil.client.StencilClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DescriptorManagerTest {

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    private StencilClient stencilClient;

    @Before
    public void setup() {
        initMocks(this);
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldReturnValidDescriptors() {
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        DescriptorManager descriptorManager = new DescriptorManager(stencilClientOrchestrator);
        Descriptors.Descriptor descriptor = descriptorManager
                .getDescriptor("org.raystack.dagger.consumer.TestBookingLogMessage");

        assertEquals(TestBookingLogMessage.getDescriptor(), descriptor);
    }

    @Test
    public void shouldReturnValidDescriptorsInCaseOfEnrichment() {
        List<String> grpcSpecificStencilURLs = Collections.singletonList("http://localhost/url");
        when(stencilClientOrchestrator.enrichStencilClient(grpcSpecificStencilURLs)).thenReturn(stencilClient);
        DescriptorManager descriptorManager = new DescriptorManager(stencilClientOrchestrator, grpcSpecificStencilURLs);
        Descriptors.Descriptor descriptor = descriptorManager
                .getDescriptor("org.raystack.dagger.consumer.TestBookingLogMessage");

        assertEquals(TestBookingLogMessage.getDescriptor(), descriptor);
    }

    @Test
    public void shouldThrowIncaseOfDescriptorNotFound() {

        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        DescriptorManager descriptorManager = new DescriptorManager(stencilClientOrchestrator);
        DescriptorNotFoundException exception = assertThrows(DescriptorNotFoundException.class, () -> descriptorManager.getDescriptor("test"));
        assertEquals("No Descriptor found for class test", exception.getMessage());

    }
}
