package io.odpf.dagger.core.processors.external.pg;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class PgSourceConfigTest {

    private final String host = "localhost";
    private final String port = "9200";
    private final String user = "user";
    private final String password = "password";
    private final String database = "db";
    private final String type = "TestMessage";
    private final String capacity = "30";
    private final String streamTimeout = "4000";
    private final Map<String, String> defaultOutputMapping = new HashMap<>();
    private final String connectTimeout = "1000";
    private final String idleTimeout = "3000";
    private final String queryVariables = "driver_id";
    private final String queryPattern = "/drivers/driver/%s";
    private PgSourceConfig pgSourceConfig;
    private final boolean failOnErrors = false;
    private final String metricId = "metricId-pg-01";
    private final boolean retainResponseType = false;

    @Before
    public void setUp() {
        pgSourceConfig = getPgSourceConfigBuilder()
                .createPgSourceConfig();

    }

    private PgSourceConfigBuilder getPgSourceConfigBuilder() {
        return new PgSourceConfigBuilder()
                .setHost(host)
                .setPort(port)
                .setUser(user)
                .setPassword(password)
                .setDatabase(database)
                .setType(type)
                .setCapacity(capacity)
                .setStreamTimeout(streamTimeout)
                .setOutputMapping(defaultOutputMapping)
                .setConnectTimeout(connectTimeout)
                .setIdleTimeout(idleTimeout)
                .setQueryVariables(queryVariables)
                .setQueryPattern(queryPattern)
                .setFailOnErrors(failOnErrors)
                .setMetricId(metricId)
                .setRetainResponseType(retainResponseType);
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
        pgSourceConfig = getPgSourceConfigBuilder()
                .setType(null)
                .createPgSourceConfig();
        assertFalse(pgSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        pgSourceConfig = getPgSourceConfigBuilder()
                .setType("")
                .createPgSourceConfig();
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
        Map<String, String> outputMapping = new HashMap<>();
        outputMapping.put("test", "test");
        pgSourceConfig = getPgSourceConfigBuilder()
                .setQueryPattern("")
                .setOutputMapping(outputMapping)
                .createPgSourceConfig();
        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class,
                () -> pgSourceConfig.validateFields());
        assertEquals("Missing required fields: [query_pattern]", illegalArgumentException.getMessage());
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
        pgSourceConfig = getPgSourceConfigBuilder()
                .setOutputMapping(Collections.emptyMap())
                .createPgSourceConfig();
        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class,
                () -> pgSourceConfig.validateFields());
        assertEquals("Missing required fields: [output_mapping]", illegalArgumentException.getMessage());
    }

}
