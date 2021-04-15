package io.odpf.dagger.processors.common;

import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.metrics.reporters.ErrorReporter;
import io.odpf.dagger.processors.types.FilterDecorator;
import io.odpf.dagger.source.ProtoDeserializer;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.List;
import java.util.stream.Collectors;

import static io.odpf.dagger.utils.Constants.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ValidRecordsDecoratorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private ErrorReporter errorReporter;

    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setUp() {
        initMocks(this);
        configuration = new Configuration();
        configuration.setString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT);
        configuration.setString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT);
        configuration.setBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT);
        configuration.setString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }

    private String[] getColumns() {
        List<String> fields = TestBookingLogMessage.getDescriptor()
                .getFields()
                .stream()
                .map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList());
        fields.add(INTERNAL_VALIDATION_FILED);
        fields.add("rowtime");
        return fields.toArray(new String[0]);
    }

    @Test
    public void shouldThrowExceptionWithBadRecord() throws Exception {
        expectedException.expectMessage("Bad Record Encountered for table `test`");
        expectedException.expect(InvalidProtocolBufferException.class);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getName(), 5, "rowtime", stencilClientOrchestrator);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0, null, "test".getBytes());
        Row invalidRow = protoDeserializer.deserialize(consumerRecord);
        ValidRecordsDecorator filter = new ValidRecordsDecorator("test", getColumns());
        filter.errorReporter = this.errorReporter;
        Assert.assertFalse(filter.filter(invalidRow));
    }

    @Test
    public void shouldReturnTrueForCorrectRecord() throws Exception {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getName(), 5, "rowtime", stencilClientOrchestrator);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("test-topic", 0, 0, null, TestBookingLogMessage.newBuilder().build().toByteArray());
        Row validRow = protoDeserializer.deserialize(consumerRecord);
        FilterDecorator filter = new ValidRecordsDecorator("test", getColumns());
        Assert.assertTrue(filter.filter(validRow));
    }
}
