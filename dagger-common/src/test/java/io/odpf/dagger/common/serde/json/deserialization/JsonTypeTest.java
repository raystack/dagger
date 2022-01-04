package io.odpf.dagger.common.serde.json.deserialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.odpf.dagger.common.core.Constants.ROWTIME;
import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class JsonTypeTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldGiveAllColumnTypesOfJsonAlongWithRowtime() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }";
        JsonType jsonType = new JsonType(jsonSchema, ROWTIME);

        assertArrayEquals(
                new TypeInformation[]{STRING, SQL_TIMESTAMP, BOOLEAN, SQL_TIMESTAMP},
                ((RowTypeInfo) jsonType.getRowType()).getFieldTypes());
    }

    @Test
    public void shouldGiveAllColumnNamesOfJsonAlongWithRowtimeAndInvalidField() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"integer\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonType jsonType = new JsonType(jsonSchema, ROWTIME);

        assertArrayEquals(
                new String[]{"id", "time", "random", "__internal_validation_field__", "rowtime"},
                ((RowTypeInfo) jsonType.getRowType()).getFieldNames());
    }

    @Test
    public void shouldGiveAllTypesOfFieldsAlongWithRowtime() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"integer\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonType jsonType = new JsonType(jsonSchema, ROWTIME);

        assertArrayEquals(
                new TypeInformation[]{STRING, BIG_DEC, BIG_DEC, BOOLEAN, SQL_TIMESTAMP},
                ((RowTypeInfo) jsonType.getRowType()).getFieldTypes());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenClassIsNotJSON() {
        thrown.expect(IllegalArgumentException.class);
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"random\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonType jsonType = new JsonType(jsonSchema, ROWTIME);

        jsonType.getRowType();
    }

    @Test
    public void shouldGiveSimpleMappedFlinkTypes() {
        String jsonSchema = "{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"integer\" }, \"random\": { \"description\": \"one random field\", \"type\": \"integer\" } }, \"required\": [ \"id\", \"time\", \"random\" ] }";
        JsonType jsonType = new JsonType(jsonSchema, ROWTIME);

        TypeInformation[] fieldTypes = ((RowTypeInfo) jsonType.getRowType()).getFieldTypes();
        String[] fieldNames = ((RowTypeInfo) jsonType.getRowType()).getFieldNames();

        assertEquals(STRING, fieldTypes[0]);
        assertEquals(BIG_DEC, fieldTypes[1]);
        assertEquals(BIG_DEC, fieldTypes[1]);

        assertEquals("id", fieldNames[0]);
        assertEquals("time", fieldNames[1]);
        assertEquals("random", fieldNames[2]);
    }

    @Test
    public void shouldThrowErrorForAdvancedJsonSchemaFeaturesWhichAreNotSupportedYet() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Only JSON schemes with simple references (one indirection in the same document) are supported yet. But was: /schemas/address");
        String jsonSchema = "{ \"$id\": \"https://example.com/schemas/customer\", \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": { \"first_name\": { \"type\": \"string\" }, \"last_name\": { \"type\": \"string\" }, \"billing_address\": { \"$ref\": \"/schemas/address\" } }, \"required\": [\"first_name\", \"last_name\", \"billing_address\"], \"$defs\": { \"address\": { \"$id\": \"/schemas/address\", \"$schema\": \"http://json-schema.org/draft-07/schema#\", \"type\": \"object\", \"properties\": { \"street_address\": { \"type\": \"string\" }, \"city\": { \"type\": \"string\" } }, \"required\": [\"street_address\", \"city\"] } } }";
        JsonType jsonType = new JsonType(jsonSchema, ROWTIME);

        TypeInformation[] fieldTypes = ((RowTypeInfo) jsonType.getRowType()).getFieldTypes();
    }

    @Test
    public void shouldProcessTypeInformationForNestedJsonSchema() {
        String jsonSchema = "{ \"$id\": \"https://example.com/schemas/customer\", \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": { \"first_name\": { \"type\": \"string\" }, \"last_name\": { \"type\": \"string\" }, \"billing_address\": { \"$id\": \"/schemas/address\", \"$schema\": \"http://json-schema.org/draft-07/schema#\", \"type\": \"object\", \"properties\": { \"street_address\": { \"type\": \"string\" }, \"city\": { \"type\": \"string\" } }, \"required\": [ \"street_address\", \"city\" ] } }, \"required\": [ \"first_name\", \"last_name\", \"billing_address\" ] }";
        JsonType jsonType = new JsonType(jsonSchema, ROWTIME);

        TypeInformation[] fieldTypes = ((RowTypeInfo) jsonType.getRowType()).getFieldTypes();
        String[] fieldNames = ((RowTypeInfo) jsonType.getRowType()).getFieldNames();

        assertEquals(STRING, fieldTypes[0]);
        assertEquals(STRING, fieldTypes[1]);
        assertEquals(ROW_NAMED(new String[]{"street_address", "city"}, STRING, STRING), fieldTypes[2]);
        assertEquals(BOOLEAN, fieldTypes[3]);
        assertEquals(SQL_TIMESTAMP, fieldTypes[4]);

        assertEquals("first_name", fieldNames[0]);
        assertEquals("last_name", fieldNames[1]);
        assertEquals("billing_address", fieldNames[2]);
        assertEquals("__internal_validation_field__", fieldNames[3]);
        assertEquals("rowtime", fieldNames[4]);
    }

    @Test
    public void shouldProcessTypeInformationForJsonSchemaWithArrayFields() {
        String jsonSchema = "{ \"$id\": \"https://example.com/schemas/customer\", \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": { \"first_names\": { \"type\": \"array\", \"items\": { \"type\": \"string\" } }, \"last_name\": { \"type\": \"string\" }, \"billing_address\": { \"$id\": \"/schemas/address\", \"$schema\": \"http://json-schema.org/draft-07/schema#\", \"type\": \"object\", \"properties\": { \"street_address\": { \"type\": \"string\" }, \"city\": { \"type\": \"string\" } }, \"required\": [ \"street_address\", \"city\" ] } }, \"required\": [ \"first_names\", \"last_name\", \"billing_address\" ] }";
        JsonType jsonType = new JsonType(jsonSchema, ROWTIME);

        TypeInformation[] fieldTypes = ((RowTypeInfo) jsonType.getRowType()).getFieldTypes();
        String[] fieldNames = ((RowTypeInfo) jsonType.getRowType()).getFieldNames();

        assertEquals(STRING_ARRAY_TYPE_INFO, fieldTypes[0]);
        assertEquals(STRING, fieldTypes[1]);
        assertEquals(ROW_NAMED(new String[]{"street_address", "city"}, STRING, STRING), fieldTypes[2]);
        assertEquals(BOOLEAN, fieldTypes[3]);
        assertEquals(SQL_TIMESTAMP, fieldTypes[4]);

        assertEquals("first_names", fieldNames[0]);
        assertEquals("last_name", fieldNames[1]);
        assertEquals("billing_address", fieldNames[2]);
        assertEquals("__internal_validation_field__", fieldNames[3]);
        assertEquals("rowtime", fieldNames[4]);
    }
}
