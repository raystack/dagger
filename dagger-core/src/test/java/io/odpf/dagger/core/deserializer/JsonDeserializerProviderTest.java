package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class JsonDeserializerProviderTest {
    @Mock
    private StreamConfig streamConfig;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldBeAbleToProvideJsonDeserializerWhenSourceNameIsKafkaAndSchemaTypeIsJSON() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        JsonDeserializerProvider provider = new JsonDeserializerProvider(streamConfig);

        assertTrue(provider.canProvide());
    }

    @Test
    public void shouldBeAbleToProvideJsonDeserializerWhenSourceNameIsKafkaConsumerAndSchemaTypeIsJSON() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        JsonDeserializerProvider provider = new JsonDeserializerProvider(streamConfig);

        assertTrue(provider.canProvide());
    }

    @Test
    public void shouldNotProvideJsonDeserializerWhenSourceNameIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        JsonDeserializerProvider provider = new JsonDeserializerProvider(streamConfig);

        assertFalse(provider.canProvide());
    }

    @Test
    public void shouldNotProvideJsonDeserializerWhenSchemaTypeIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        JsonDeserializerProvider provider = new JsonDeserializerProvider(streamConfig);

        assertFalse(provider.canProvide());
    }

    @Test
    public void shouldReturnJsonDeserializerForSupportedSourceNameAndSchemaType() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");
        when(streamConfig.getJsonSchema()).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");

        JsonDeserializerProvider provider = new JsonDeserializerProvider(streamConfig);
        DaggerDeserializer<Row> daggerDeserializer = provider.getDaggerDeserializer();

        assertTrue(daggerDeserializer instanceof JsonDeserializer);
    }
}
