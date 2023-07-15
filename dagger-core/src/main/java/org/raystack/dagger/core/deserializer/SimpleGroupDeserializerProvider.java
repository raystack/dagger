package org.raystack.dagger.core.deserializer;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.serde.DaggerDeserializer;
import org.raystack.dagger.common.serde.DataTypes;
import org.raystack.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import org.raystack.dagger.core.source.config.StreamConfig;
import org.raystack.dagger.core.source.config.models.SourceDetails;
import org.raystack.dagger.core.source.config.models.SourceName;
import org.raystack.dagger.core.utils.Constants;
import org.apache.flink.types.Row;

import static org.raystack.dagger.common.serde.DataTypes.PROTO;

public class SimpleGroupDeserializerProvider implements DaggerDeserializerProvider<Row> {
    protected final StreamConfig streamConfig;
    protected final Configuration configuration;
    protected final StencilClientOrchestrator stencilClientOrchestrator;
    private static final SourceName COMPATIBLE_SOURCE = SourceName.PARQUET_SOURCE;
    private static final DataTypes COMPATIBLE_INPUT_SCHEMA_TYPE = PROTO;

    public SimpleGroupDeserializerProvider(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
    }

    @Override
    public DaggerDeserializer<Row> getDaggerDeserializer() {
        int timestampFieldIndex = Integer.parseInt(streamConfig.getEventTimestampFieldIndex());
        String protoClassName = streamConfig.getProtoClass();
        String rowTimeAttributeName = configuration.getString(Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
        return new SimpleGroupDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClientOrchestrator);
    }

    @Override
    public boolean canProvide() {
        SourceDetails[] sourceDetailsList = streamConfig.getSourceDetails();
        for (SourceDetails sourceDetails : sourceDetailsList) {
            SourceName sourceName = sourceDetails.getSourceName();
            DataTypes inputSchemaType = DataTypes.valueOf(streamConfig.getDataType());
            if (!sourceName.equals(COMPATIBLE_SOURCE) || !inputSchemaType.equals(COMPATIBLE_INPUT_SCHEMA_TYPE)) {
                return false;
            }
        }
        return true;
    }
}
