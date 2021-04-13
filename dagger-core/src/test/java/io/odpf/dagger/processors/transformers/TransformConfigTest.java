package io.odpf.dagger.processors.transformers;

import org.junit.Assert;
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

    private TransformConfig transformConfig;
    private String transformationClass;
    private Map<String, Object> transformationArguments;

    @Before
    public void setUp() {
        transformationClass = "com.gojek.daggers.postprocessor.XTransformer";
        transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "key");
        transformationArguments.put("valueColumnName", "value");

        transformConfig = new TransformConfig(transformationClass, transformationArguments);
    }

    @Test
    public void shouldReturnTransformationClass() {
        Assert.assertEquals("com.gojek.daggers.postprocessor.XTransformer", transformConfig.getTransformationClass());
    }

    @Test
    public void shouldReturnTransformationArguments() {
        Assert.assertEquals(transformationArguments, transformConfig.getTransformationArguments());
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
        expectedMandatoryFields.put("transformationClass", "com.gojek.daggers.postprocessor.XTransformer");
        HashMap<String, Object> actualMandatoryFields = transformConfig.getMandatoryFields();
        assertEquals(expectedMandatoryFields.get("transformationClass"), actualMandatoryFields.get("transformationClass"));
    }

    @Test
    public void shouldThrowExceptionIfDefaultFieldsAreOverridden() {
        expectedException.expectMessage("Transformation arguments cannot contain `table_name` as a key");
        expectedException.expect(IllegalArgumentException.class);
        TransformConfig config = new TransformConfig("com.gojek.TestClass", new HashMap<String, Object>() {{
            put(TransformerUtils.DefaultArgument.TABLE_NAME.toString(), "test-value");
        }});
        config.validateFields();
    }
}
