package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.external.SchemaConfig;
import com.gojek.de.stencil.client.StencilClient;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FetchOutputDecoratorTest {

    @Mock
    private SchemaConfig schemaConfig;

    @Mock
    private DataStream<Row> inputDataStream;

    @Mock
    private SingleOutputStreamOperator<Row> outputDataStream;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private ColumnNameManager columnNameManager;

    private String[] outputColumnNames;

    @Before
    public void setup() {
        initMocks(this);
        outputColumnNames = new String[]{"order_number", "service_type"};
        when(schemaConfig.getOutputProtoClassName()).thenReturn("TestProtoClass");
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        when(schemaConfig.getColumnNameManager()).thenReturn(columnNameManager);
        when(columnNameManager.getOutputColumnNames()).thenReturn(outputColumnNames);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("TestProtoClass")).thenReturn(TestBookingLogMessage.getDescriptor());

    }

    @Test
    public void canDecorateShouldBeFalse() {
        Assert.assertFalse(new FetchOutputDecorator(schemaConfig, false).canDecorate());
    }

    @Test
    public void shouldMapOutputDataFromRowManager() {
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, false);
        Row parentRow = new Row(2);
        Row inputRow = new Row(3);
        Row outputRow = new Row(4);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);

        assertEquals(outputRow, fetchOutputDecorator.map(parentRow));
    }

    @Test
    public void shouldDecorateStreamWithItsMapFunction() {
        when(inputDataStream.map(any(MapFunction.class))).thenReturn(outputDataStream);
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, false);
        fetchOutputDecorator.decorate(inputDataStream);
        verify(inputDataStream, times(1)).map(fetchOutputDecorator);
    }

    @Test
    public void shouldNotReturnTypeInformationIfSqlProcessorNotEnabled() {
        when(inputDataStream.map(any(MapFunction.class))).thenReturn(outputDataStream);
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, false);
        fetchOutputDecorator.decorate(inputDataStream);
        verify(inputDataStream, times(1)).map(fetchOutputDecorator);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING}, outputColumnNames);
        verify(outputDataStream, times(0)).returns(rowTypeInfo);
    }

    @Test
    public void shouldDecorateStreamAndReturnTypesIfAllFieldsInOutputProtoIfSqlProcessorEnabled() {
        when(inputDataStream.map(any(MapFunction.class))).thenReturn(outputDataStream);
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, true);
        fetchOutputDecorator.decorate(inputDataStream);
        verify(inputDataStream, times(1)).map(fetchOutputDecorator);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING}, outputColumnNames);
        verify(outputDataStream, times(1)).returns(rowTypeInfo);
    }

    @Test
    public void shouldDecorateStreamAndReturnTypesAsObjectIfDescriptorNullIfSqlProcessorEnabled() {
        when(stencilClient.get("TestProtoClass")).thenReturn(null);
        when(inputDataStream.map(any(MapFunction.class))).thenReturn(outputDataStream);
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, true);
        fetchOutputDecorator.decorate(inputDataStream);
        verify(inputDataStream, times(1)).map(fetchOutputDecorator);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Object.class), TypeInformation.of(Object.class)}, outputColumnNames);
        verify(outputDataStream, times(1)).returns(rowTypeInfo);
    }

    @Test
    public void shouldDecorateStreamAndReturnTypesAsObjectIfSomeFieldsNotPresentInDescriptorIfSqlProcessorEnabled() {
        outputColumnNames = new String[]{"order_number", "service_type", "test_field"};
        when(columnNameManager.getOutputColumnNames()).thenReturn(outputColumnNames);
        when(inputDataStream.map(any(MapFunction.class))).thenReturn(outputDataStream);
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, true);
        fetchOutputDecorator.decorate(inputDataStream);
        verify(inputDataStream, times(1)).map(fetchOutputDecorator);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, TypeInformation.of(Object.class)}, outputColumnNames);
        verify(outputDataStream, times(1)).returns(rowTypeInfo);
    }

    @Test
    public void shouldDecorateStreamAndReturnTypesAsObjectsIfAllFieldsNotPresentInDescriptorIfSqlProcessorEnabled() {
        outputColumnNames = new String[]{"test1", "test2", "test3"};
        when(columnNameManager.getOutputColumnNames()).thenReturn(outputColumnNames);
        when(inputDataStream.map(any(MapFunction.class))).thenReturn(outputDataStream);
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, true);
        fetchOutputDecorator.decorate(inputDataStream);
        verify(inputDataStream, times(1)).map(fetchOutputDecorator);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Object.class), TypeInformation.of(Object.class), TypeInformation.of(Object.class)}, outputColumnNames);
        verify(outputDataStream, times(1)).returns(rowTypeInfo);
    }

    @Test
    public void shouldDecorateStreamAndReturnTypesHavingRowtimeAsSqlTimestampIfSqlProcessorEnabled() {
        outputColumnNames = new String[]{"order_number", "service_type", "rowtime"};
        when(columnNameManager.getOutputColumnNames()).thenReturn(outputColumnNames);
        when(inputDataStream.map(any(MapFunction.class))).thenReturn(outputDataStream);
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator(schemaConfig, true);
        fetchOutputDecorator.decorate(inputDataStream);
        verify(inputDataStream, times(1)).map(fetchOutputDecorator);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.SQL_TIMESTAMP}, outputColumnNames);
        verify(outputDataStream, times(1)).returns(rowTypeInfo);
    }
}
