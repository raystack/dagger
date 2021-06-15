package io.odpf.dagger.core.processors.transformers;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TransformConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        assertEquals("io.odpf.daggers.postprocessor.XTransformer", defaultTransformConfig.getTransformationClass());
    }

    @Test
    public void shouldReturnTransformationArguments() {
        assertEquals(transformationArguments, defaultTransformConfig.getTransformationArguments());
    }

    @Test
    public void shouldThrowExceptionIfMandatoryFieldsAreMissing() {
        expectedException.expectMessage("Missing required fields: [transformationClass]");
        expectedException.expect(IllegalArgumentException.class);

        TransformConfig transformConfig = new TransformConfig(null, null);
        transformConfig.validateFields();
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
        expectedException.expectMessage("Transformation arguments cannot contain `table_name` as a key");
        expectedException.expect(IllegalArgumentException.class);
        TransformConfig config = new TransformConfig("io.odpf.TestClass", new HashMap<String, Object>() {{
            put(TransformerUtils.DefaultArgument.INPUT_SCHEMA_TABLE.toString(), "test-value");
        }});
        config.validateFields();
    }
}
