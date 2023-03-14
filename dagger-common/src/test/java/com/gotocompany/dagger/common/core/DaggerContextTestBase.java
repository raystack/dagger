package com.gotocompany.dagger.common.core;

import com.gotocompany.dagger.common.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public abstract class DaggerContextTestBase {

    protected DaggerContext daggerContext;

    protected DataStream<Row> inputStream;

    protected StreamExecutionEnvironment streamExecutionEnvironment;

    protected StreamTableEnvironment streamTableEnvironment;

    protected Configuration configuration;

    @Before
    public void setupBase() {
        daggerContext = mock(DaggerContext.class);
        configuration = mock(Configuration.class);
        inputStream = mock(DataStream.class);
        streamExecutionEnvironment = mock(StreamExecutionEnvironment.class);
        streamTableEnvironment = mock(StreamTableEnvironment.class);
        initMocks(this);
        setMock(daggerContext);
        when(daggerContext.getConfiguration()).thenReturn(configuration);
        when(inputStream.getExecutionEnvironment()).thenReturn(streamExecutionEnvironment);
        when(daggerContext.getExecutionEnvironment()).thenReturn(streamExecutionEnvironment);
        when(daggerContext.getTableEnvironment()).thenReturn(streamTableEnvironment);
    }

    @After
    public void resetSingletonBase() throws Exception {
        Field instance = DaggerContext.class.getDeclaredField("daggerContext");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    private void setMock(DaggerContext mock) {
        try {
            Field instance = DaggerContext.class.getDeclaredField("daggerContext");
            instance.setAccessible(true);
            instance.set(instance, mock);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
