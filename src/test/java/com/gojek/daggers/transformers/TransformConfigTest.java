package com.gojek.daggers.transformers;

import com.gojek.daggers.postprocessors.transfromers.TransformConfig;
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
        expectedException.expectMessage("Missing required fields: [transformationClass, transformationArguments]");
        expectedException.expect(IllegalArgumentException.class);

        TransformConfig transformConfig = new TransformConfig(null, null);
        transformConfig.validateFields();
    }

    @Test
    public void shouldReturnMandatoryFields() {
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();
        expectedMandatoryFields.put("transformationClass", "com.gojek.daggers.postprocessor.XTransformer");
        expectedMandatoryFields.put("transformationArguments", "transformationArguments");
        HashMap<String, Object> actualMandatoryFields = transformConfig.getMandatoryFields();
        assertEquals(expectedMandatoryFields.get("transformationClass"), actualMandatoryFields.get("transformationClass"));
        assertEquals(transformationArguments.get("keyColumnName"), ((Map<String, String>) actualMandatoryFields.get("transformationArguments")).get("keyColumnName"));
        assertEquals(transformationArguments.get("valueColumnName"), ((Map<String, String>) actualMandatoryFields.get("transformationArguments")).get("valueColumnName"));
    }
}
