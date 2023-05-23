package com.gotocompany.dagger.functions.transformers;

import com.google.protobuf.Timestamp;
import com.gotocompany.dagger.common.core.DaggerContextTestBase;
import com.gotocompany.dagger.common.core.StreamInfo;
import com.gotocompany.dagger.common.exceptions.DescriptorNotFoundException;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.functions.exceptions.InvalidHashFieldException;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HashTransformerTest extends DaggerContextTestBase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @Mock
    private org.apache.flink.configuration.Configuration flinkInternalConfig;

    @Before
    public void setup() {

        initMocks(this);
        when(configuration.getString("SINK_KAFKA_PROTO_MESSAGE", ""))
                .thenReturn("com.gotocompany.dagger.consumer.TestBookingLogMessage");
        when(configuration.getBoolean("SCHEMA_REGISTRY_STENCIL_ENABLE", false))
                .thenReturn(false);
        when(configuration.getString("SCHEMA_REGISTRY_STENCIL_URLS", ""))
                .thenReturn("");
        when(configuration.getBoolean("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH", false))
                .thenReturn(false);
        when(configuration.getLong("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS", TimeUnit.HOURS.toMillis(2)))
                .thenReturn(TimeUnit.HOURS.toMillis(2));
        when(configuration.getString("SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY", "LONG_POLLING"))
                .thenReturn("LONG_POLLING");

    }

    @Test
    public void shouldHashSingleStringFieldInInputRow() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();

        ArrayList<String> fieldsToEncrypt = new ArrayList<String>();
        fieldsToEncrypt.add("order_number");

        transformationArguments.put("maskColumns", fieldsToEncrypt);
        transformationArguments.put("valueColumnName", fieldsToEncrypt);
        String[] columnNames = {"order_number", "cancel_reason_id", "is_reblast"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1);
        inputRow.setField(2, false);

        HashTransformer hashTransformer = new HashTransformer(transformationArguments, columnNames, daggerContext);
        hashTransformer.open(flinkInternalConfig);

        Row outputRow = hashTransformer.map(inputRow);

        Assert.assertEquals(3, outputRow.getArity());
        Assert.assertNotEquals(inputRow.getField(0), outputRow.getField(0));
        Assert.assertEquals(inputRow.getField(1), outputRow.getField(1));
        Assert.assertEquals(inputRow.getField(2), outputRow.getField(2));
        Assert.assertEquals("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
                outputRow.getField(0));
    }

    @Test
    public void shouldHashMultipleFieldsOfSupportedDatatypeInInputRow() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();

        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("order_number");
        fieldsToEncrypt.add("cancel_reason_id");
        transformationArguments.put("maskColumns", fieldsToEncrypt);

        transformationArguments.put("valueColumnName", fieldsToEncrypt);
        String[] columnNames = {"order_number", "cancel_reason_id", "is_reblast"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1);
        inputRow.setField(2, false);

        HashTransformer hashTransformer = new HashTransformer(transformationArguments, columnNames, daggerContext);
        hashTransformer.open(flinkInternalConfig);

        Row outputRow = hashTransformer.map(inputRow);

        Assert.assertEquals(3, outputRow.getArity());
        Assert.assertNotEquals(inputRow.getField(0), outputRow.getField(0));
        Assert.assertNotEquals(inputRow.getField(1), outputRow.getField(1));
        Assert.assertEquals(inputRow.getField(2), outputRow.getField(2));
        Assert.assertEquals("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
                outputRow.getField(0));
        Assert.assertEquals(1927129959, outputRow.getField(1));
    }

    @Test
    public void shouldHashAllFieldsOfSupportedDataTypesInInputRow() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();

        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("order_number");
        fieldsToEncrypt.add("driver_total_fare_without_surge");
        fieldsToEncrypt.add("cancel_reason_id");

        transformationArguments.put("maskColumns", fieldsToEncrypt);

        transformationArguments.put("valueColumnName", fieldsToEncrypt);
        String[] columnNames = {"order_number", "cancel_reason_id", "driver_total_fare_without_surge"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1);
        inputRow.setField(2, 1L);

        HashTransformer hashTransformer = new HashTransformer(transformationArguments, columnNames, daggerContext);
        hashTransformer.open(flinkInternalConfig);

        Row outputRow = hashTransformer.map(inputRow);

        Assert.assertEquals(3, outputRow.getArity());
        Assert.assertNotEquals(inputRow.getField(0), outputRow.getField(0));
        Assert.assertNotEquals(inputRow.getField(1), outputRow.getField(1));
        Assert.assertNotEquals(inputRow.getField(2), outputRow.getField(2));
        Assert.assertEquals("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
                outputRow.getField(0));
        Assert.assertEquals(1927129959, outputRow.getField(1));
        Assert.assertEquals(-6467378160175308932L, outputRow.getField(2));
    }

    @Test
    public void shouldHashNestedFields() throws Exception {
        when(configuration.getString("SINK_KAFKA_PROTO_MESSAGE", ""))
                .thenReturn("com.gotocompany.dagger.consumer.TestEnrichedBookingLogMessage");
        HashMap<String, Object> transformationArguments = new HashMap<>();

        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("booking_log.event_timestamp.seconds");
        fieldsToEncrypt.add("booking_log.event_timestamp.nanos");
        fieldsToEncrypt.add("booking_log.order_number");

        transformationArguments.put("maskColumns", fieldsToEncrypt);

        transformationArguments.put("valueColumnName", fieldsToEncrypt);
        String[] columnNames = {"booking_log"};

        Row inputRow = new Row(3);

        Row bookingLogRow = new Row(TestBookingLogMessage.getDescriptor().getFields().size());
        Row eventTimestampRow = new Row(Timestamp.getDescriptor().getFields().size());

        eventTimestampRow.setField(0, 10L);
        eventTimestampRow.setField(1, 10);

        bookingLogRow.setField(1, "test_order_number");
        bookingLogRow.setField(2, "test_order_url");
        bookingLogRow.setField(4, eventTimestampRow);

        inputRow.setField(0, bookingLogRow);

        HashTransformer hashTransformer = new HashTransformer(transformationArguments, columnNames, daggerContext);
        hashTransformer.open(flinkInternalConfig);

        Row outputRow = hashTransformer.map(inputRow);

        Assert.assertEquals(3, outputRow.getArity());
        Assert.assertEquals("4f901e546da47e1266355bcc4953ed452ffcdd2eeec5567c0abe677879f6d47a",
                ((Row) outputRow.getField(0)).getField(1));
        Assert.assertEquals("test_order_url",
                ((Row) outputRow.getField(0)).getField(2));
        Assert.assertEquals(-8613927256589200991L, ((Row) ((Row) outputRow.getField(0)).getField(4)).getField(0));
        Assert.assertEquals(-1176347385, ((Row) ((Row) outputRow.getField(0)).getField(4)).getField(1));
    }

    @Test
    public void shouldCreateRowHasherMapInOpen() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();

        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("order_number");

        transformationArguments.put("maskColumns", fieldsToEncrypt);
        transformationArguments.put("valueColumnName", fieldsToEncrypt);
        String[] columnNames = {"order_number", "cancel_reason_id", "is_reblast"};

        HashTransformer hashTransformer = new HashTransformer(transformationArguments, columnNames, daggerContext);
        hashTransformer.open(flinkInternalConfig);

        verify(configuration, times(1)).getString("SINK_KAFKA_PROTO_MESSAGE", "");
    }


    @Test
    public void shouldThrowErrorIfUnableToCreateRowHasherMap() throws Exception {
        thrown.expect(InvalidHashFieldException.class);
        thrown.expectMessage("No primitive field found for hashing");
        HashMap<String, Object> transformationArguments = new HashMap<>();

        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("invalid_field");

        transformationArguments.put("maskColumns", fieldsToEncrypt);
        transformationArguments.put("valueColumnName", fieldsToEncrypt);
        String[] columnNames = {"order_number", "cancel_reason_id", "is_reblast"};

        HashTransformer hashTransformer = new HashTransformer(transformationArguments, columnNames, daggerContext);
        hashTransformer.open(flinkInternalConfig);
    }

    @Test
    public void shouldThrowErrorIfUnableToFindOpDescriptor() throws Exception {
        when(configuration.getString("SINK_KAFKA_PROTO_MESSAGE", ""))
                .thenReturn("com.gotocompany.dagger.consumer.RandomTestMessage");
        thrown.expect(DescriptorNotFoundException.class);
        thrown.expectMessage("Output Descriptor for class: com.gotocompany.dagger.consumer.RandomTestMessage not found");
        HashMap<String, Object> transformationArguments = new HashMap<>();

        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("invalid_field");

        transformationArguments.put("maskColumns", fieldsToEncrypt);
        transformationArguments.put("valueColumnName", fieldsToEncrypt);
        String[] columnNames = {"order_number", "cancel_reason_id", "is_reblast"};

        HashTransformer hashTransformer = new HashTransformer(transformationArguments, columnNames, daggerContext);
        hashTransformer.open(flinkInternalConfig);
    }

    @Test
    public void shouldTransformInputStreamToOutputStream() {
        HashMap<String, Object> transformationArguments = new HashMap<>();

        ArrayList<String> fieldsToEncrypt = new ArrayList<>();
        fieldsToEncrypt.add("order_number");

        transformationArguments.put("maskColumns", fieldsToEncrypt);
        transformationArguments.put("valueColumnName", fieldsToEncrypt);
        String[] columnNames = {"order_number", "cancel_reason_id", "is_reblast"};

        HashTransformer hashTransformer = new HashTransformer(transformationArguments, columnNames, daggerContext);

        hashTransformer.transform(new StreamInfo(inputStream, columnNames));
        verify(inputStream, times(1)).map(hashTransformer);
    }
}
