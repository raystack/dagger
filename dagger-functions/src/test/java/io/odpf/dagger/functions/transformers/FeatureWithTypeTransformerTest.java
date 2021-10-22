package io.odpf.dagger.functions.transformers;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class FeatureWithTypeTransformerTest {

    @Mock
    private DataStream<Row> dataStream;

    @Mock
    private org.apache.flink.configuration.Configuration flinkInternalConfig;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnOutputRowAsFeatureRowOnPassingStringAsValue() throws Exception {
        HashMap<String, Object> transfromationArguments = new HashMap<>();
        transfromationArguments.put("outputColumnName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "StringType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        transfromationArguments.put("data", hashMaps);

        String[] columnNames = {"customer_id", "order_number", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "test_order_number");
        inputRow.setField(2, "test_features");
        FeatureWithTypeTransformer featureWithTypeTransformer = new FeatureWithTypeTransformer(transfromationArguments, columnNames, configuration);
        Row outputRow = featureWithTypeTransformer.map(inputRow);
        Assert.assertEquals(3, outputRow.getArity());
        Assert.assertEquals(inputRow.getField(0), outputRow.getField(0));
        Assert.assertEquals(inputRow.getField(1), outputRow.getField(1));

        Row[] features = (Row[]) outputRow.getField(2);
        Row feature = (Row) features[0].getField(1);
        Assert.assertEquals("test_order_number", feature.getField(1));
    }

    @Test
    public void shouldReturnOutputRowAsFeatureRowOnPassingFloatType() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("outputColumnName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "FloatType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        transformationArguments.put("data", hashMaps);

        String[] columnNames = {"customer_id", "order_number", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "1.4");
        inputRow.setField(2, "test_features");
        FeatureWithTypeTransformer featureWithTypeTransformer = new FeatureWithTypeTransformer(transformationArguments, columnNames, configuration);
        Row outputRow = featureWithTypeTransformer.map(inputRow);
        Assert.assertEquals(3, outputRow.getArity());
        Assert.assertEquals(inputRow.getField(0), outputRow.getField(0));
        Assert.assertEquals(inputRow.getField(1), outputRow.getField(1));


        Row[] features = (Row[]) outputRow.getField(2);
        Row feature = (Row) features[0].getField(1);
        Assert.assertEquals(1.4f, feature.getField(5));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForUnSupportedDataType() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("outputColumnName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "BytType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        transformationArguments.put("data", hashMaps);

        String[] columnNames = {"customer_id", "order_number", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "value".getBytes());
        inputRow.setField(2, "test_features");
        FeatureWithTypeTransformer featureWithTypeTransformer = new FeatureWithTypeTransformer(transformationArguments, columnNames, configuration);
        featureWithTypeTransformer.map(inputRow);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfKeyColumnNameNotExist() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("outputColumnName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "test");
        data.put("valueColumnName", "order_number");
        data.put("type", "StringType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        transformationArguments.put("data", hashMaps);

        String[] columnNames = {"customer_id", "order_number", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "value".getBytes());
        inputRow.setField(2, "test_features");
        FeatureWithTypeTransformer featureWithTypeTransformer = new FeatureWithTypeTransformer(transformationArguments, columnNames, configuration);
        featureWithTypeTransformer.map(inputRow);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfValueColumnNameNotExist() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("outputColumnName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "test_value");
        data.put("type", "StringType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        transformationArguments.put("data", hashMaps);

        String[] columnNames = {"customer_id", "order_number", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "value".getBytes());
        inputRow.setField(2, "test_features");
        FeatureWithTypeTransformer featureWithTypeTransformer = new FeatureWithTypeTransformer(transformationArguments, columnNames, configuration);
        featureWithTypeTransformer.map(inputRow);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfVoutputColumnNameNotExist() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("outputColumnName", "random");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "StringType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        transformationArguments.put("data", hashMaps);

        String[] columnNames = {"customer_id", "order_number", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "value".getBytes());
        inputRow.setField(2, "test_features");
        FeatureWithTypeTransformer featureWithTypeTransformer = new FeatureWithTypeTransformer(transformationArguments, columnNames, configuration);
        featureWithTypeTransformer.map(inputRow);
    }

    @Test
    public void shouldTransformInputStreamWithItsTransformer() {
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("outputColumnaName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "FloatType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        transformationArguments.put("data", hashMaps);

        String[] columnNames = {"customer_id", "order_number", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "1.4");
        inputRow.setField(2, "test_features");
        FeatureWithTypeTransformer featureWithTypeTransformer = new FeatureWithTypeTransformer(transformationArguments, columnNames, configuration);
        StreamInfo inputStreamInfo = new StreamInfo(dataStream, columnNames);
        featureWithTypeTransformer.transform(inputStreamInfo);
        verify(dataStream, times(1)).map(any(FeatureWithTypeTransformer.class));
    }

    @Test
    public void shouldReturnSameColumnNames() {
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("outputColumnaName", "features");
        HashMap<String, String> data = new HashMap<>();
        data.put("keyColumnName", "customer_id");
        data.put("valueColumnName", "order_number");
        data.put("type", "FloatType");
        List<HashMap<String, String>> hashMaps = Collections.singletonList(data);
        transformationArguments.put("data", hashMaps);

        String[] columnNames = {"customer_id", "order_number", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test_customer_id");
        inputRow.setField(1, "1.4");
        inputRow.setField(2, "test_features");
        FeatureWithTypeTransformer featureWithTypeTransformer = new FeatureWithTypeTransformer(transformationArguments, columnNames, configuration);
        StreamInfo inputStreamInfo = new StreamInfo(dataStream, columnNames);
        StreamInfo outputStreamInfo = featureWithTypeTransformer.transform(inputStreamInfo);
        Assert.assertArrayEquals(columnNames, outputStreamInfo.getColumnNames());
    }
}
