package io.odpf.dagger.core.source;

import io.odpf.stencil.client.StencilClient;
import com.google.gson.JsonSyntaxException;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;


import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamsFactoryTest {
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldThrowErrorForInvalidStreamConfig() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \\\"EARLIEST_TIME_URL_FIRST,"
                        + "\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-05/hr=09/\",  \"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-03/hr=14/\"],"
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        assertThrows(JsonSyntaxException.class,
                () -> StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator));
    }

    @Test
    public void shouldThrowNullPointerIfStreamConfigIsNotGiven() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("");

        assertThrows(NullPointerException.class,
                () -> StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator));
    }
}
