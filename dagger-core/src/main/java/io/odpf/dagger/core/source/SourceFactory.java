package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.source.parquet.*;
import io.odpf.dagger.core.source.parquet.reader.PrimitiveReaderProvider;
import io.odpf.dagger.core.source.parquet.reader.ReaderProvider;
import io.odpf.dagger.core.source.parquet.splitassigner.ChronologyOrderedSplitAssigner;
import io.odpf.dagger.core.source.parquet.splitassigner.IndexOrderedSplitAssigner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.function.Supplier;

import static io.odpf.dagger.core.source.SourceType.BOUNDED;

public class SourceFactory {

    public static Source create(SourceDetails sourceDetails,
                                StreamConfig streamConfig,
                                Configuration configuration,
                                DaggerDeserializer<Row> deserializer) {
        SourceType sourceType = sourceDetails.getSourceType();
        SourceName sourceName = sourceDetails.getSourceName();
        switch (sourceName) {
            case PARQUET: {
                SimpleGroupDeserializer simpleGroupDeserializer = (SimpleGroupDeserializer) deserializer;
                ParquetFileSource parquetFileSource = buildParquetFileSource(sourceType,
                        streamConfig,
                        configuration,
                        simpleGroupDeserializer);
                return parquetFileSource.buildFileSource();
            }
            case KAFKA: {
                KafkaDeserializationSchema<Row> deserializationSchema = (KafkaDeserializationSchema<Row>) deserializer;
                KafkaRecordDeserializationSchema<Row> recordDeserializationSchema = KafkaRecordDeserializationSchema.of(deserializationSchema);
                return buildKafkaSource(sourceType, streamConfig, configuration, recordDeserializationSchema);
            }
            default: {
                String message = String.format("Invalid stream configuration: No suitable Flink DataSource could be " +
                        "constructed for source %s", sourceName.name());
                throw new DaggerConfigurationException(message);
            }
        }
    }

    private static KafkaSource<Row> buildKafkaSource(SourceType sourceType, StreamConfig streamConfig, Configuration configuration, KafkaRecordDeserializationSchema<Row> deserializer) {
        if (sourceType == BOUNDED) {
            throw new DaggerConfigurationException("Running KafkaSource in BOUNDED mode is not supported yet");
        }
        return KafkaSource.<Row>builder()
                .setTopicPattern(streamConfig.getTopicPattern())
                .setStartingOffsets(streamConfig.getStartingOffset())
                .setProperties(streamConfig.getKafkaProps(configuration))
                .setDeserializer(deserializer)
                .build();
    }

    private static ParquetFileSource buildParquetFileSource(SourceType sourceType,
                                                    StreamConfig streamConfig,
                                                    Configuration configuration,
                                                    SimpleGroupDeserializer simpleGroupDeserializer) {

        ParquetFileSource.Builder parquetFileSourceBuilder = ParquetFileSource.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = buildParquetFileRecordFormat(simpleGroupDeserializer);
        FileSplitAssigner.Provider splitAssignerProvider = buildParquetFileSplitAssignerProvider(streamConfig);
        Path[] filePaths = buildFlinkFilePaths(streamConfig);

        return parquetFileSourceBuilder.setFilePaths(filePaths)
                .setConfiguration(configuration)
                .setFileRecordFormat(parquetFileRecordFormat)
                .setSourceType(sourceType)
                .setFileSplitAssigner(splitAssignerProvider)
                .build();
    }

    private static Path[] buildFlinkFilePaths(StreamConfig streamConfig) {
        String[] gcsParquetFilePaths = streamConfig.getParquetFilePaths();
        return Arrays.stream(gcsParquetFilePaths)
                .map(Path::new)
                .toArray(Path[]::new);
    }

    private static FileSplitAssigner.Provider buildParquetFileSplitAssignerProvider(StreamConfig streamConfig) {
        SourceParquetReadOrderStrategy readOrderStrategy = streamConfig.getParquetFilesReadOrderStrategy();
        switch (readOrderStrategy) {
            case EARLIEST_TIME_URL_FIRST:
                return ChronologyOrderedSplitAssigner::new;
            case EARLIEST_INDEX_FIRST:
                return IndexOrderedSplitAssigner::new;
            default:
                return LocalityAwareSplitAssigner::new;
        }
    }

    private static ParquetFileRecordFormat buildParquetFileRecordFormat(SimpleGroupDeserializer simpleGroupDeserializer) {
        ReaderProvider parquetFileReaderProvider = new PrimitiveReaderProvider(simpleGroupDeserializer);
        ParquetFileRecordFormat.Builder parquetFileRecordFormatBuilder = ParquetFileRecordFormat.Builder.getInstance();
        Supplier<TypeInformation<Row>> typeInformationProvider = simpleGroupDeserializer::getProducedType;
        return parquetFileRecordFormatBuilder
                .setParquetFileReaderProvider(parquetFileReaderProvider)
                .setTypeInformationProvider(typeInformationProvider)
                .build();
    }
}