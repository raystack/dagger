package io.odpf.dagger.core.processors.external.pg;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PgSourceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private String host;
    private String port;
    private String user;
    private String password;
    private String database;
    private String type;
    private String capacity;
    private String streamTimeout;
    private Map<String, String> defaultOutputMapping;
    private String connectTimeout;
    private String idleTimeout;
    private String queryVariables;
    private String queryPattern;
    private PgSourceConfig pgSourceConfig;
    private boolean failOnErrors;
    private String metricId;
    private boolean retainResponseType;

    @Before
    public void setUp() {

        host = "localhost";
        port = "9200";
        user = "user";
        password = "password";
        database = "db";
        type = "TestMessage";
        capacity = "30";
        streamTimeout = "4000";
        defaultOutputMapping = new HashMap<>();
        queryPattern = "/drivers/driver/%s";
        queryVariables = "driver_id";
        connectTimeout = "1000";
        idleTimeout = "3000";
        metricId = "metricId-pg-01";
        failOnErrors = false;
        retainResponseType = false;
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, type, capacity, streamTimeout, defaultOutputMapping, connectTimeout, idleTimeout, queryVariables,
                queryPattern, failOnErrors, metricId, retainResponseType);

    }

    @Test
    public void getHostShouldGetRightConfig() {
        assertEquals(host, pgSourceConfig.getHost());
    }

    @Test
    public void getPortShouldGetRightConfig() {
        assertEquals(Integer.valueOf(port), pgSourceConfig.getPort());
    }

    @Test
    public void getUserShouldGetRightConfig() {
        assertEquals(user, pgSourceConfig.getUser());
    }

    @Test
    public void getPasswordShouldGetRightConfig() {
        assertEquals(password, pgSourceConfig.getPassword());
    }

    @Test
    public void getDatabaseShouldGetRightConfig() {
        assertEquals(database, pgSourceConfig.getDatabase());
    }

    @Test
    public void getQueryPatternShouldGetRightConfig() {
        assertEquals(queryPattern, pgSourceConfig.getPattern());
    }

    @Test
    public void getQueryVariablesShouldGetRightConfig() {
        assertEquals(queryVariables, pgSourceConfig.getQueryVariables());
    }

    @Test
    public void getTypeShouldGetRightConfig() {
        assertEquals(type, pgSourceConfig.getType());
    }

    @Test
    public void hasTypeShouldBeTrueWhenTypeIsPresent() {
        assertTrue(pgSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsNull() {
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, null, capacity, streamTimeout, defaultOutputMapping, connectTimeout, idleTimeout, queryVariables,
                queryPattern, failOnErrors, metricId, retainResponseType);
        assertFalse(pgSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, "", capacity, streamTimeout, defaultOutputMapping, connectTimeout, idleTimeout, queryVariables,
                queryPattern, failOnErrors, metricId, retainResponseType);
        assertFalse(pgSourceConfig.hasType());
    }

    @Test
    public void getCapacityShouldGetRightConfig() {
        assertEquals(Integer.valueOf(capacity), pgSourceConfig.getCapacity());
    }

    @Test
    public void getIdleTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(idleTimeout), pgSourceConfig.getIdleTimeout());
    }

    @Test
    public void getStreamTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(streamTimeout), pgSourceConfig.getStreamTimeout());
    }

    @Test
    public void getConnectTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(connectTimeout), pgSourceConfig.getConnectTimeout());
    }

    @Test
    public void isRetainResponseTypeShouldGetTheRightConfig() {
        assertEquals(retainResponseType, pgSourceConfig.isRetainResponseType());
    }

    @Test
    public void getOutputColumnNames() {
        List<String> keys = new ArrayList<>();
        keys.add("outputField");
        defaultOutputMapping.put("outputField", "param");
        assertEquals(keys, pgSourceConfig.getOutputColumns());
    }

    @Test
    public void shouldReturnPathForOutputField() {
        defaultOutputMapping.put("outputField", "param");
        assertEquals("param", pgSourceConfig.getMappedQueryParam("outputField"));
    }

    @Test
    public void shouldThrowExceptionIfRequestPatternIsEmpty() {
        expectedException.expectMessage("Missing required fields: [query_pattern]");
        expectedException.expect(IllegalArgumentException.class);
        Map<String, String> outputMapping = new HashMap<>();
        outputMapping.put("test", "test");
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, "", capacity, streamTimeout, outputMapping, connectTimeout, idleTimeout, queryVariables,
                "", failOnErrors, metricId, retainResponseType);
        pgSourceConfig.validateFields();
    }

    @Test
    public void shouldReturnMandatoryFields() {
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();
        expectedMandatoryFields.put("host", host);
        expectedMandatoryFields.put("port", port);
        expectedMandatoryFields.put("user", user);
        expectedMandatoryFields.put("password", password);
        expectedMandatoryFields.put("database", database);
        expectedMandatoryFields.put("capacity", capacity);
        expectedMandatoryFields.put("stream_timeout", streamTimeout);
        expectedMandatoryFields.put("connect_timeout", connectTimeout);
        expectedMandatoryFields.put("idle_timeout", idleTimeout);
        expectedMandatoryFields.put("query_pattern", queryPattern);
        expectedMandatoryFields.put("output_mapping", defaultOutputMapping);
        expectedMandatoryFields.put("fail_on_errors", failOnErrors);
        HashMap<String, Object> actualMandatoryFields = pgSourceConfig.getMandatoryFields();
        assertArrayEquals(expectedMandatoryFields.values().toArray(), actualMandatoryFields.values().toArray());
    }

    @Test
    public void shouldValidateWhenOutputMappingIsEmpty() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [output_mapping]");
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, type, capacity, streamTimeout,
                new HashMap<>(), connectTimeout, idleTimeout, queryVariables, queryPattern, true, metricId, retainResponseType);

        pgSourceConfig.validateFields();
    }

}
