package com.gojek.daggers.postProcessors.external.pg;

import org.junit.Assert;
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
    private Map<String, String> outputMapping;
    private String maximumConnectionPoolSize;
    private String connectTimeout;
    private String idleTimeout;
    private String queryVariables;
    private String queryPattern;
    private PgSourceConfig pgSourceConfig;
    private boolean failOnErrors;

    public void setup() {

    }


    @Before
    public void setUp() throws Exception {

        host = "localhost";
        port = "9200";
        user = "user";
        password = "password";
        database = "db";
        type = "com.gojek.esb.fraud.DriverProfileFlattenLogMessage";
        capacity = "30";
        streamTimeout = "4000";
        outputMapping = new HashMap<>();
        queryPattern = "/drivers/driver/%s";
        queryVariables = "driver_id";
        connectTimeout = "1000";
        maximumConnectionPoolSize = "20";
        idleTimeout = "3000";
        failOnErrors = false;
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, type, capacity, streamTimeout, outputMapping, maximumConnectionPoolSize, connectTimeout, idleTimeout, queryVariables,
                queryPattern, failOnErrors);

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
    public void getMaxConnectionPoolShouldGetRightConfig() {
        assertEquals(Integer.valueOf(maximumConnectionPoolSize), pgSourceConfig.getMaximumConnectionPoolSize());
    }

    @Test
    public void getQueryPatternShouldGetRightConfig() {
        assertEquals(queryPattern, pgSourceConfig.getQueryPattern());
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
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, null, capacity, streamTimeout, outputMapping, maximumConnectionPoolSize, connectTimeout, idleTimeout, queryVariables,
                queryPattern, failOnErrors);
        assertFalse(pgSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, "", capacity, streamTimeout, outputMapping, maximumConnectionPoolSize, connectTimeout, idleTimeout, queryVariables,
                queryPattern, failOnErrors);
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
    public void getOutputColumnNames() {
        List<String> keys = new ArrayList<>();
        keys.add("outputField");
        outputMapping.put("outputField", "param");
        assertEquals(keys, pgSourceConfig.getOutputColumns());
    }

    @Test
    public void shouldReturnPathForOutputField() {
        outputMapping.put("outputField", "param");
        Assert.assertEquals("param", pgSourceConfig.getMappedQueryParam("outputField"));
    }

    @Test
    public void shouldReturnMandatoryFields() {
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();
        expectedMandatoryFields.put("host", host);
        expectedMandatoryFields.put("port", port);
        expectedMandatoryFields.put("user", user);
        expectedMandatoryFields.put("password", password);
        expectedMandatoryFields.put("database", database);
        expectedMandatoryFields.put("type", type);
        expectedMandatoryFields.put("capacity", capacity);
        expectedMandatoryFields.put("stream_timeout", streamTimeout);
        expectedMandatoryFields.put("maximum_connection_pool_size", maximumConnectionPoolSize);
        expectedMandatoryFields.put("connect_timeout", connectTimeout);
        expectedMandatoryFields.put("idle_timeout", idleTimeout);
        expectedMandatoryFields.put("query_pattern", queryPattern);
        expectedMandatoryFields.put("query_variables", queryVariables);
        expectedMandatoryFields.put("output_mapping", outputMapping);
        expectedMandatoryFields.put("fail_on_errors", failOnErrors);
        HashMap<String, Object> actualMandatoryFields = pgSourceConfig.getMandatoryFields();
        assertArrayEquals(expectedMandatoryFields.values().toArray(), actualMandatoryFields.values().toArray());
    }

    @Test
    public void shouldValidateWhenOutputMappingIsEmpty() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [output_mapping]");
        pgSourceConfig = new PgSourceConfig(host, port, user, password, database, type, capacity, streamTimeout,
                new HashMap<>(), maximumConnectionPoolSize, connectTimeout, idleTimeout, queryVariables, queryPattern, true);

        pgSourceConfig.validateFields();
    }

}