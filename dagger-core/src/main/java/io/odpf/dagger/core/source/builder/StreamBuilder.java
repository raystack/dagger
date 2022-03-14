package io.odpf.dagger.core.source.builder;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.deserializer.DeserializerFactory;
import io.odpf.dagger.core.source.*;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class StreamBuilder implements TelemetryPublisher {

    private StreamConfig streamConfig;
    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;

    public StreamBuilder(StreamConfig streamConfig, Configuration configuration) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.stencilClientOrchestrator = null;
    }

    public StreamBuilder(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addTelemetry();
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return getMetrics();
    }

    abstract void addTelemetry();

    abstract Map<String, List<String>> getMetrics();

    public abstract boolean canBuild();

    public Stream buildStream() {
        return new Stream(createDataSource(), streamConfig.getSourceDetails(), streamConfig.getSchemaTable(), getInputDataType());
    }

    private Source createDataSource() {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        ArrayList<Source> sourceList = new ArrayList<>();
        for (SourceDetails sourceDetails : sourceDetailsArray) {
            SourceName sourceName = sourceDetails.getSourceName();
            DataTypes inputDataType = getInputDataType();
            DaggerDeserializer<Row> deserializer = DeserializerFactory.create(sourceName, inputDataType, streamConfig, configuration, stencilClientOrchestrator);
            Source source = SourceFactory.create(sourceDetails, streamConfig, configuration, deserializer);
            sourceList.add(source);
        }
        if(sourceList.size() > 1) {
            throw new IllegalConfigurationException("Invalid stream configuration: Multiple back to back data sources is not supported yet.");
        }
        return sourceList.get(0);
    }

    DataTypes getInputDataType() {
        return DataTypes.valueOf(streamConfig.getDataType());
    }
}