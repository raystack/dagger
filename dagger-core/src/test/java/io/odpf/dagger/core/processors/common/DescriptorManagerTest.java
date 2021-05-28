package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DescriptorManagerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
                .getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");

        Assert.assertEquals(TestBookingLogMessage.getDescriptor(), descriptor);
    }

    @Test
    public void shouldReturnValidDescriptorsInCaseOfEnrichment() {
        List<String> grpcSpecificStencilURLs = Collections.singletonList("http://localhost/url");
        when(stencilClientOrchestrator.enrichStencilClient(grpcSpecificStencilURLs)).thenReturn(stencilClient);
        DescriptorManager descriptorManager = new DescriptorManager(stencilClientOrchestrator, grpcSpecificStencilURLs);
        Descriptors.Descriptor descriptor = descriptorManager
                .getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");

        Assert.assertEquals(TestBookingLogMessage.getDescriptor(), descriptor);
    }

    @Test
    public void shouldThrowIncaseOfDescriptorNotFound() {
        expectedException.expect(DescriptorNotFoundException.class);
        expectedException.expectMessage("No Descriptor found for class test");

        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        DescriptorManager descriptorManager = new DescriptorManager(stencilClientOrchestrator);
        descriptorManager.getDescriptor("test");
    }
}
