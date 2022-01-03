package io.odpf.dagger.common.serde.json.deserialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.math.BigDecimal;

import static org.apache.flink.api.common.typeinfo.Types.BIG_DEC;
import static org.apache.flink.api.common.typeinfo.Types.BOOLEAN;
import static org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;
import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.MockitoAnnotations.initMocks;

public class JsonDeserializerTest {

    @Mock
    private Configuration configuration;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldAlwaysReturnFalseForEndOfStream() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }";
        assertFalse(new JsonDeserializer(jsonSchema, "time").isEndOfStream(null));
    }

    @Test
    public void shouldReturnProducedType() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"integer\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");
        TypeInformation<Row> producedType = jsonDeserializer.getProducedType();
        assertArrayEquals(
                new String[]{"id", "time", "random", "__internal_validation_field__", "rowtime"},
                ((RowTypeInfo) producedType).getFieldNames());
        assertArrayEquals(
                new TypeInformation[]{STRING, BIG_DEC, BIG_DEC, BOOLEAN, SQL_TIMESTAMP},
                ((RowTypeInfo) producedType).getFieldTypes());
    }

    @Test
    public void shouldDeserializeSimpleFieldsOfAJsonData() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"integer\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");

        byte[] data = "{ \"time\": 1637829201, \"id\": \"001\", \"random\": 1, \"name\": \"Cake\" }".getBytes();

        Row row = jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, data));

        int size = row.getArity();

        assertEquals(5, size);

        assertEquals("001", row.getField(0));
        assertEquals(new BigDecimal("1637829201"), row.getField(1));
        assertEquals(new BigDecimal("1"), row.getField(2));
    }

    @Test
    public void shouldAddExtraFieldsToRow() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"integer\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");

        byte[] data = "{ \"time\": 1637829201, \"id\": \"001\", \"random\": 1, \"name\": \"Cake\" }".getBytes();

        Row row = jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, data));

        assertEquals(true, row.getField(row.getArity() - 2));
        assertEquals(1637829201000L, ((java.sql.Timestamp) row.getField(row.getArity() - 1)).getTime());
    }

    @Test
    public void shouldDeserializeJSONAsSubRows() {
        String jsonSchema = "{ \"$id\": \"https://example.com/schemas/customer\", \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": { \"first_name\": { \"type\": \"string\" }, \"last_name\": { \"type\": \"string\" }, \"time\": { \"type\": \"integer\" }, \"billing_address\": { \"$id\": \"/schemas/address\", \"$schema\": \"http://json-schema.org/draft-07/schema#\", \"type\": \"object\", \"properties\": { \"street_address\": { \"type\": \"string\" }, \"city\": { \"type\": \"string\" } }, \"required\": [ \"street_address\", \"city\" ] } }, \"required\": [ \"first_name\", \"last_name\", \"time\", \"billing_address\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");

        byte[] data = "{ \"time\": 1637829201, \"first_name\": \"james\", \"last_name\": \"bondu\", \"billing_address\": { \"street_address\": \"random\", \"city\": \"blr\" } }".getBytes();

        Row row = jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, data));

        Row nestedAddressRow = (Row) row.getField(3);
        assertEquals("random", nestedAddressRow.getField(0));
        assertEquals("blr", nestedAddressRow.getField(1));
    }

    @Test
    public void shouldDeserializeArrayOfString() {
        String jsonSchema = "{ \"$id\": \"https://example.com/schemas/customer\", \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": { \"first_names\": { \"type\": \"array\", \"items\": { \"type\": \"string\" } }, \"last_name\": { \"type\": \"string\" }, \"time\": { \"type\": \"integer\" }, \"billing_address\": { \"$id\": \"/schemas/address\", \"$schema\": \"http://json-schema.org/draft-07/schema#\", \"type\": \"object\", \"properties\": { \"street_address\": { \"type\": \"string\" }, \"city\": { \"type\": \"string\" } }, \"required\": [ \"street_address\", \"city\" ] } }, \"required\": [ \"first_names\", \"last_name\", \"time\", \"billing_address\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");

        byte[] data = "{ \"time\": 1637829201, \"first_names\": [\"james\", \"arujit\"], \"last_name\": \"bondu\", \"billing_address\": { \"street_address\": \"random\", \"city\": \"blr\" } }".getBytes();

        Row row = jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, data));

        String[] nameFields = (String[]) row.getField(0);
        assertArrayEquals(new String[]{"james", "arujit"}, nameFields);
    }


    @Test
    public void shouldThrowExceptionForNullData() {
        String jsonSchema = "{ \"$id\": \"https://example.com/schemas/customer\", \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": { \"first_names\": { \"type\": \"array\", \"items\": { \"type\": \"string\" } }, \"last_name\": { \"type\": \"string\" }, \"time\": { \"type\": \"integer\" }, \"billing_address\": { \"$id\": \"/schemas/address\", \"$schema\": \"http://json-schema.org/draft-07/schema#\", \"type\": \"object\", \"properties\": { \"street_address\": { \"type\": \"string\" }, \"city\": { \"type\": \"string\" } }, \"required\": [ \"street_address\", \"city\" ] } }, \"required\": [ \"first_names\", \"last_name\", \"time\", \"billing_address\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");

        assertThrows(DaggerDeserializationException.class,
                () -> jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, null)));
    }

    @Test
    public void shouldThrowExceptionIfRowTimeFieldsNotFound() {
        String jsonSchema = "{ \"$id\": \"https://example.com/schemas/customer\", \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": { \"first_names\": { \"type\": \"array\", \"items\": { \"type\": \"string\" } }, \"last_name\": { \"type\": \"string\" }, \"time\": { \"type\": \"integer\" }, \"billing_address\": { \"$id\": \"/schemas/address\", \"$schema\": \"http://json-schema.org/draft-07/schema#\", \"type\": \"object\", \"properties\": { \"street_address\": { \"type\": \"string\" }, \"city\": { \"type\": \"string\" } }, \"required\": [ \"street_address\", \"city\" ] } }, \"required\": [ \"first_names\", \"last_name\", \"time\", \"billing_address\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");

        byte[] data = "{ \"first_names\": [\"james\", \"arujit\"], \"last_name\": \"bondu\", \"billing_address\": { \"street_address\": \"random\", \"city\": \"blr\" } }".getBytes();
        assertThrows(DaggerDeserializationException.class,
                () -> jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, data)));
    }


    @Test
    public void shouldThrowExceptionForUnMatchingJsonSchema() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"integer\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"integer\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");

        byte[] data = "{ \"time\": 1637829201, \"first_names\": [ \"james\", \"arujit\" ], \"id\": \"random_id\", \"billing_address\": { \"street_address\": \"random\", \"city\": \"blr\" } }".getBytes();
        assertThrows(DaggerDeserializationException.class,
                () -> jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, data)));
    }

    @Test
    public void shouldPopulateBigDecimalRowTimeFieldToRow() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"integer\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "time");

        byte[] data = "{ \"time\": 1637829201, \"id\": \"001\", \"random\": 1, \"name\": \"Cake\" }".getBytes();

        Row row = jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, data));

        assertEquals(1637829201000L, ((java.sql.Timestamp) row.getField(row.getArity() - 1)).getTime());
    }

    @Test
    public void shouldThrowErrorForUnSupportedRowTimeField() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"booking\", \"description\": \"a booking\", \"type\": \"object\", \"properties\": { \"order_number\": { \"type\": \"string\" }, \"order_url\": { \"type\": \"string\" }, \"event_timestamp\": { \"type\": \"string\", \"format\" : \"date-time\" }, \"driver_id\": { \"type\": \"string\" }, \"total_distance_in_kms\": { \"type\": \"number\" }, \"time\": { \"type\": \"integer\" } }, \"required\": [ \"order_number\", \"event_timestamp\", \"driver_id\" ]}";
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, "driver_id");

        byte[] data = "{ \"order_number\": \"test_order_3\", \"driver_id\": \"test_driver_1\", \"event_timestamp\": \"2021-12-16T14:57:00Z\" }".getBytes();

        assertThrows(DaggerDeserializationException.class,
                () -> jsonDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, data)));
    }
}
