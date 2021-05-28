package io.odpf.dagger.functions.transformers.feature;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

public class FeatureWithTypeHandlerTest {

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnOutputColumnIndex() {
        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
        stringObjectHashMap.put("outputColumnName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "StingType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        stringObjectHashMap.put("data", hashMaps);

        String[] inputColumnNames = {"customer_id", "order_number", "features"};
        FeatureWithTypeHandler featureWithTypeHandler = new FeatureWithTypeHandler(stringObjectHashMap, inputColumnNames);
        int outputColumnIndex = featureWithTypeHandler.getOutputColumnIndex();

        Assert.assertEquals(2, outputColumnIndex);
    }


    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfOutputColumnNotPresent() {
        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
        stringObjectHashMap.put("outputColumnName", "x");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "StingType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        stringObjectHashMap.put("data", hashMaps);

        String[] inputColumnNames = {"customer_id", "order_number", "features"};
        FeatureWithTypeHandler featureWithTypeHandler = new FeatureWithTypeHandler(stringObjectHashMap, inputColumnNames);
        featureWithTypeHandler.getOutputColumnIndex();
    }

    @Test
    public void shouldPopulateFeatures() {
        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
        stringObjectHashMap.put("outputColumnName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "StringType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        stringObjectHashMap.put("data", hashMaps);

        String[] inputColumnNames = {"customer_id", "order_number", "features"};
        FeatureWithTypeHandler featureWithTypeHandler = new FeatureWithTypeHandler(stringObjectHashMap, inputColumnNames);

        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "test_order_number");
        inputRow.setField(2, "test_features");

        ArrayList<Row> featureRow = featureWithTypeHandler.populateFeatures(inputRow);
        Assert.assertEquals(1, featureRow.size());
        Assert.assertEquals("test_customer_id", featureRow.get(0).getField(0));
    }
}
