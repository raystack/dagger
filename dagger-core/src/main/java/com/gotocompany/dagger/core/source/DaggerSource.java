package com.gotocompany.dagger.core.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface DaggerSource<T> {
    DataStream<T> register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<T> watermarkStrategy);

    boolean canBuild();
}
