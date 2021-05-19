package io.odpf.dagger.functions.transformers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static io.odpf.dagger.functions.transformers.filter.FilterAspects.FILTERED_INVALID_RECORDS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class InvalidRecordFilterTransformerTest {
    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private MetricGroup metricGroup;
    @Mock
    private Counter counter;

    @Before
    public void setUp() {
        initMocks(this);
    }

    private static class StubCounter implements Answer<Integer> {
        private int ct = 0;

        @Override
        public Integer answer(InvocationOnMock invocation) throws Throwable {
            return ++ct;
        }
    }

    private String[] getColumns() {
        List<String> fields = TestBookingLogMessage.getDescriptor()
                .getFields()
                .stream()
                .map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList());
        fields.add(InvalidRecordFilterTransformer.INTERNAL_VALIDATION_FILED);
        fields.add("rowtime");
        return fields.toArray(new String[0]);
    }

    private Row createDefaultInvalidRow(DynamicMessage defaultInstance) {
        List<Descriptors.FieldDescriptor> descriptorFields = defaultInstance.getDescriptorForType().getFields();
        Row row = new Row(descriptorFields.size() + 2);
        row.setField(row.getArity() - 2, false);
        row.setField(row.getArity() - 1, new Timestamp(0));
        return row;
    }

    private Row createDefaultValidRow(DynamicMessage defaultInstance) {
        List<Descriptors.FieldDescriptor> descriptorFields = defaultInstance.getDescriptorForType().getFields();
        Row row = new Row(descriptorFields.size() + 2);
        row.setField(row.getArity() - 2, true);
        row.setField(row.getArity() - 1, new Timestamp(System.currentTimeMillis()));
        return row;
    }

    @Test
    public void shouldFilterBadRecords() throws Exception {
        Configuration configuration = new Configuration();
        InvalidRecordFilterTransformer filter = new InvalidRecordFilterTransformer(new HashMap<String, Object>() {{
            put("table_name", "test");
        }}, getColumns(), configuration);
        filter.setRuntimeContext(runtimeContext);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("per_table", "test")).thenReturn(metricGroup);
        when(metricGroup.counter(FILTERED_INVALID_RECORDS.getValue())).thenReturn(counter);
        StubCounter ct = new StubCounter();
        doAnswer(ct).when(counter).inc();
        Row invalidRow = createDefaultInvalidRow(DynamicMessage.getDefaultInstance(TestBookingLogMessage.getDescriptor()));
        filter.open(configuration);
        Assert.assertFalse(filter.filter(invalidRow));
        Assert.assertEquals(1, ct.ct);
        Assert.assertFalse(filter.filter(invalidRow));
        Assert.assertEquals(2, ct.ct);
        Assert.assertFalse(filter.filter(invalidRow));
        Assert.assertEquals(3, ct.ct);
    }

    @Test
    public void shouldPassValidRecords() throws Exception {
        Configuration configuration = new Configuration();
        InvalidRecordFilterTransformer filter = new InvalidRecordFilterTransformer(new HashMap<String, Object>() {{
            put("table_name", "test");
        }}, getColumns(), configuration);
        filter.setRuntimeContext(runtimeContext);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("per_table", "test")).thenReturn(metricGroup);
        when(metricGroup.counter(FILTERED_INVALID_RECORDS.getValue())).thenReturn(counter);
        StubCounter ct = new StubCounter();
        doAnswer(ct).when(counter).inc();
        Row validRow = createDefaultValidRow(DynamicMessage.getDefaultInstance(TestBookingLogMessage.getDescriptor()));
        filter.open(configuration);
        Assert.assertTrue(filter.filter(validRow));
        Assert.assertEquals(0, ct.ct);
    }
}
