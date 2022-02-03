package io.odpf.dagger.functions.transformers;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class FeatureTransformerTest {

    @Mock
    private DataStream<Row> dataStream;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnOutputRowAsFeatureRowOnPassingFloatAsValue() throws Exception {
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "featuresKey");
        transformationArguments.put("valueColumnName", "features");
        String[] columnNames = {"entityKey", "featuresKey", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1L);
        inputRow.setField(2, 2f);
        FeatureTransformer featureTransformer = new FeatureTransformer(transformationArguments, columnNames, configuration);
        Row outputRow = featureTransformer.map(inputRow);
        Assert.assertEquals(3, outputRow.getArity());
        Assert.assertEquals(inputRow.getField(0), outputRow.getField(0));
        Assert.assertEquals(inputRow.getField(1), outputRow.getField(1));
        Row[] features = (Row[]) outputRow.getField(2);
        Row feature = (Row) features[0].getField(1);
        Assert.assertEquals(inputRow.getField(2), feature.getField(5));
    }


    @Test
    public void shouldReturnOutputRowAsFeatureRowOnPassingIntegerAsValue() throws Exception {
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "featuresKey");
        transformationArguments.put("valueColumnName", "features");
        String[] columnNames = {"entityKey", "featuresKey", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1L);
        inputRow.setField(2, 2);
        FeatureTransformer featureTransformer = new FeatureTransformer(transformationArguments, columnNames, configuration);
        Row outputRow = featureTransformer.map(inputRow);
        Assert.assertEquals(3, outputRow.getArity());
        Assert.assertEquals(inputRow.getField(0), outputRow.getField(0));
        Assert.assertEquals(inputRow.getField(1), outputRow.getField(1));
        Row[] features = (Row[]) outputRow.getField(2);
        Row feature = (Row) features[0].getField(1);
        Assert.assertEquals(inputRow.getField(2), feature.getField(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForUnSupportedDataType() throws Exception {
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "featuresKey");
        transformationArguments.put("valueColumnName", "features");
        String[] columnNames = {"entityKey", "featuresKey", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1L);
        inputRow.setField(2, "value".getBytes());
        FeatureTransformer featureTransformer = new FeatureTransformer(transformationArguments, columnNames, configuration);
        featureTransformer.map(inputRow);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldHandleWhenKeyIsNotPresent() throws Exception {
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("invalidKeyName", "featuresKey");
        transformationArguments.put("valueColumnName", "features");
        String[] columnNames = {"entityKey", "featuresKey", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1L);
        inputRow.setField(2, 2f);
        FeatureTransformer featureTransformer = new FeatureTransformer(transformationArguments, columnNames, configuration);
        featureTransformer.map(inputRow);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldHandleWhenValueIsNotPresent() throws Exception {
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "featuresKey");
        transformationArguments.put("invalidColumnName", "features");
        String[] columnNames = {"entityKey", "featuresKey", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1L);
        inputRow.setField(2, 2f);
        FeatureTransformer featureTransformer = new FeatureTransformer(transformationArguments, columnNames, configuration);
        featureTransformer.map(inputRow);
    }

    @Test
    public void shouldTransformInputStreamWithItsTransformer() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "featuresKey");
        transformationArguments.put("valueColumnName", "features");
        String[] columnNames = {"entityKey", "featuresKey", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1L);
        inputRow.setField(2, 2);
        FeatureTransformer featureTransformer = new FeatureTransformer(transformationArguments, columnNames, configuration);
        StreamInfo inputStreamInfo = new StreamInfo(dataStream, columnNames);
        featureTransformer.transform(inputStreamInfo);
        verify(dataStream, times(1)).map(any(FeatureTransformer.class));
    }

    @Test
    public void shouldReturnSameColumnNames() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "featuresKey");
        transformationArguments.put("valueColumnName", "features");
        String[] columnNames = {"entityKey", "featuresKey", "features"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "test");
        inputRow.setField(1, 1L);
        inputRow.setField(2, 2);
        FeatureTransformer featureTransformer = new FeatureTransformer(transformationArguments, columnNames, configuration);
        StreamInfo inputStreamInfo = new StreamInfo(dataStream, columnNames);
        StreamInfo outputStreamInfo = featureTransformer.transform(inputStreamInfo);
        Assert.assertArrayEquals(columnNames, outputStreamInfo.getColumnNames());
    }
}
