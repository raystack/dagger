package io.odpf.dagger.core.processors.common;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.types.FilterDecorator;
import io.odpf.dagger.core.source.ProtoDeserializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;
import java.util.stream.Collectors;

import static io.odpf.dagger.common.core.Constants.*;
import static io.odpf.dagger.core.utils.Constants.INTERNAL_VALIDATION_FILED_KEY;
import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ValidRecordsDecoratorTest {

    @Mock
    private ErrorReporter errorReporter;

    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setUp() {
        initMocks(this);
        configuration = new Configuration();
        configuration.setString(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT);
        configuration.setBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        configuration.setString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }

    private String[] getColumns() {
        List<String> fields = TestBookingLogMessage.getDescriptor()
                .getFields()
                .stream()
                .map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList());
        fields.add(INTERNAL_VALIDATION_FILED_KEY);
        fields.add("rowtime");
        return fields.toArray(new String[0]);
    }

    @Test
    public void shouldThrowExceptionWithBadRecord() throws Exception {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getName(), 5, "rowtime", stencilClientOrchestrator);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0, null, "test".getBytes());
        Row invalidRow = protoDeserializer.deserialize(consumerRecord);
        ValidRecordsDecorator filter = new ValidRecordsDecorator("test", getColumns());
        filter.errorReporter = this.errorReporter;
        InvalidProtocolBufferException exception = assertThrows(InvalidProtocolBufferException.class, () -> filter.filter(invalidRow));
        assertEquals("Bad Record Encountered for table `test`", exception.getMessage());
    }

    @Test
    public void shouldReturnTrueForCorrectRecord() throws Exception {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getName(), 5, "rowtime", stencilClientOrchestrator);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0, null, TestBookingLogMessage.newBuilder().build().toByteArray());
        Row validRow = protoDeserializer.deserialize(consumerRecord);
        FilterDecorator filter = new ValidRecordsDecorator("test", getColumns());
        assertTrue(filter.filter(validRow));
    }
}
