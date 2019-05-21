package com.gojek.daggers;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class StreamManagerTest {

    @Mock
    private StreamExecutionEnvironment env;

    @Mock
    private Configuration configuration;

    @Mock
    private ExecutionConfig executionConfig;

    @Mock
    private CheckpointConfig checkpointConfig;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldRegisterRequiredConfigsOnExecutionEnvironement() {
        StreamManager streamManager = new StreamManager(configuration, env);
        when(env.getConfig()).thenReturn(executionConfig);
        when(env.getCheckpointConfig()).thenReturn(checkpointConfig);
        when(configuration.getInteger("PARALLELISM", 1)).thenReturn(1);
        when(configuration.getInteger("WATERMARK_INTERVAL_MS", 10000)).thenReturn(10000);
        when(configuration.getLong("CHECKPOINT_INTERVAL", 30000)).thenReturn(30000L);
        when(configuration.getLong("CHECKPOINT_TIMEOUT", 900000)).thenReturn(900000L);
        when(configuration.getLong("CHECKPOINT_MIN_PAUSE", 5000)).thenReturn(5000L);
        when(configuration.getInteger("MAX_CONCURRECT_CHECKPOINTS", 1)).thenReturn(1);

        streamManager.registerConfigs();

        verify(env, Mockito.times(1)).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        verify(env, Mockito.times(1)).setParallelism(1);
        verify(env, Mockito.times(1)).enableCheckpointing(30000);
        verify(executionConfig, Mockito.times(1)).setAutoWatermarkInterval(10000);
        verify(executionConfig, Mockito.times(1)).setGlobalJobParameters(configuration);
        verify(checkpointConfig, Mockito.times(1)).setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        verify(checkpointConfig, Mockito.times(1)).setCheckpointTimeout(900000L);
        verify(checkpointConfig, Mockito.times(1)).setMinPauseBetweenCheckpoints(5000L);
        verify(checkpointConfig, Mockito.times(1)).setMaxConcurrentCheckpoints(1);
    }

}