package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.types.Row;

import static io.odpf.dagger.common.serde.DataTypes.PROTO;
import static io.odpf.dagger.core.source.SourceName.PARQUET_SOURCE;
import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_KEY;

public class SimpleGroupDeserializerProvider implements DaggerDeserializerProvider<Row> {
    protected final StreamConfig streamConfig;
    protected final Configuration configuration;
    protected final StencilClientOrchestrator stencilClientOrchestrator;
    private static final SourceName COMPATIBLE_SOURCE = PARQUET_SOURCE;
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
        String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
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
