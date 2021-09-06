package io.odpf.dagger.core.processors.transformers;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TransformConfigTest {

    private TransformConfig defaultTransformConfig;
    private String transformationClass;
    private Map<String, Object> transformationArguments;

    @Before
    public void setUp() {
        transformationClass = "io.odpf.daggers.postprocessor.XTransformer";
        transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "key");
        transformationArguments.put("valueColumnName", "value");

        defaultTransformConfig = new TransformConfig(transformationClass, transformationArguments);
    }

    @Test
    public void shouldReturnTransformationClass() {
        TransformConfig transformConfig = new TransformConfig(transformationClass, Collections.unmodifiableMap(transformationArguments));
        assertEquals("io.odpf.daggers.postprocessor.XTransformer", transformConfig.getTransformationClass());
    }

    @Test
    public void shouldReturnTransformationArguments() {
        TransformConfig transformConfig = new TransformConfig(transformationClass, Collections.unmodifiableMap(transformationArguments));
        assertEquals(transformationArguments, transformConfig.getTransformationArguments());
    }

    @Test
    public void shouldThrowExceptionIfMandatoryFieldsAreMissing() {

        TransformConfig transformConfig = new TransformConfig(null, null);
        IllegalArgumentException argumentException = assertThrows(IllegalArgumentException.class, () -> transformConfig.validateFields());
        assertEquals("Missing required fields: [transformationClass]", argumentException.getMessage());
    }

    @Test
    public void shouldReturnMandatoryFields() {
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();
        expectedMandatoryFields.put("transformationClass", "io.odpf.daggers.postprocessor.XTransformer");
        HashMap<String, Object> actualMandatoryFields = defaultTransformConfig.getMandatoryFields();
        assertEquals(expectedMandatoryFields.get("transformationClass"), actualMandatoryFields.get("transformationClass"));
    }

    @Test
    public void shouldThrowExceptionIfDefaultFieldsAreOverridden() {
        TransformConfig config = new TransformConfig("io.odpf.TestClass", new HashMap<String, Object>() {{
            put(TransformerUtils.DefaultArgument.INPUT_SCHEMA_TABLE.toString(), "test-value");
        }});
        IllegalArgumentException argumentException = assertThrows(IllegalArgumentException.class, () -> config.validateFields());
        assertEquals("Transformation arguments cannot contain `table_name` as a key", argumentException.getMessage());
    }
}
