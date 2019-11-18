package com.gojek.daggers.postProcessors;

import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.LongbowProcessor;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static com.gojek.daggers.utils.Constants.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class PostProcessorFactoryTest {

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClient stencilClient;

    private String[] columnNames = {"a", "b", "longbow_duration"};

    @Before
    public void setup() {
        initMocks(this);
    }


    @Test
    public void shouldReturnLongbowProcessor() {
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("longbow_key");
        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(false);

        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClient, columnNames);

        Assert.assertEquals(1, postProcessors.size());
        Assert.assertEquals(LongbowProcessor.class, postProcessors.get(0).getClass());
    }

    @Test
    public void shouldReturnParentPostProcessor() {
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("test-sql");
        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(true);

        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClient, columnNames);

        Assert.assertEquals(1, postProcessors.size());
        Assert.assertEquals(ParentPostProcessor.class, postProcessors.get(0).getClass());
    }
//
//
//    @Test
//    public void shouldReturnExternalSourceProcessor() {
//        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
//        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("test-sql");
//        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(true);
//        when(configuration.getString(POST_PROCESSOR_CONFIG_KEY, "")).thenReturn("{\"external_source\":{\"http\":[]}}");
//
//        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClient, columnNames);
//
//        Assert.assertEquals(1, postProcessors.size());
//        Assert.assertEquals(ExternalPostProcessor.class, postProcessors.get(0).getClass());
//    }
//
//    @Test
//    public void shouldReturnTransformerProcessor() {
//        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
//        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("test-sql");
//        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(true);
//        when(configuration.getString(POST_PROCESSOR_CONFIG_KEY, "")).thenReturn("{\"transformers\":[]}");
//
//        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClient, columnNames);
//
//        Assert.assertEquals(1, postProcessors.size());
//        Assert.assertEquals(TransformProcessor.class, postProcessors.get(0).getClass());
//    }
//
//    @Test
//    public void shouldReturnBothExternalSourceProcessorAndTransformerProcessor() {
//        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
//        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("test-sql");
//        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(true);
//        when(configuration.getString(POST_PROCESSOR_CONFIG_KEY, "")).thenReturn("{\"external_source\":{\"http\":[]}, \"transformers\" : []}");
//
//        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClient, columnNames);
//
//        Assert.assertEquals(2, postProcessors.size());
//        Assert.assertEquals(ExternalPostProcessor.class, postProcessors.get(0).getClass());
//        Assert.assertEquals(TransformProcessor.class, postProcessors.get(1).getClass());
//    }
//
//
//    @Test
//    public void shouldNotReturnAnyPostProcessor() {
//        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
//        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("test-sql");
//        when(configuration.getBoolean(POST_PROCESSOR_ENABLED_KEY, POST_PROCESSOR_ENABLED_KEY_DEFAULT)).thenReturn(false);
//        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClient, columnNames);
//
//        Assert.assertEquals(0, postProcessors.size());
//    }
}