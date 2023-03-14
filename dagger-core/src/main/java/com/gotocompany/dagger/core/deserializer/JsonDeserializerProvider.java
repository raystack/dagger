package com.gotocompany.dagger.core.deserializer;

import com.gotocompany.dagger.common.serde.DaggerDeserializer;
import com.gotocompany.dagger.common.serde.DataTypes;
import com.gotocompany.dagger.common.serde.json.deserialization.JsonDeserializer;
import com.gotocompany.dagger.core.source.config.StreamConfig;
import com.gotocompany.dagger.core.source.config.models.SourceDetails;
import com.gotocompany.dagger.core.source.config.models.SourceName;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashSet;

import static com.gotocompany.dagger.common.serde.DataTypes.JSON;

public class JsonDeserializerProvider implements DaggerDeserializerProvider<Row> {
    private final StreamConfig streamConfig;
    private static final HashSet<SourceName> COMPATIBLE_SOURCES = new HashSet<>(Arrays.asList(SourceName.KAFKA_SOURCE, SourceName.KAFKA_CONSUMER));
    private static final DataTypes COMPATIBLE_INPUT_SCHEMA_TYPE = JSON;

    public JsonDeserializerProvider(StreamConfig streamConfig) {
        this.streamConfig = streamConfig;
    }

    @Override
    public DaggerDeserializer<Row> getDaggerDeserializer() {
        return new JsonDeserializer(streamConfig.getJsonSchema(), streamConfig.getJsonEventTimestampFieldName());
    }

    @Override
    public boolean canProvide() {
        SourceDetails[] sourceDetailsList = streamConfig.getSourceDetails();
        for (SourceDetails sourceDetails : sourceDetailsList) {
            SourceName sourceName = sourceDetails.getSourceName();
            DataTypes inputSchemaType = DataTypes.valueOf(streamConfig.getDataType());
            if (!COMPATIBLE_SOURCES.contains(sourceName) || !inputSchemaType.equals(COMPATIBLE_INPUT_SCHEMA_TYPE)) {
                return false;
            }
        }
        return true;
    }
}
