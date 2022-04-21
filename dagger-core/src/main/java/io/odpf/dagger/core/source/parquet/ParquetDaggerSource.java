package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.dagger.core.source.DaggerSource;
import io.odpf.dagger.core.source.parquet.reader.ParquetReader;
import io.odpf.dagger.core.source.parquet.reader.ReaderProvider;
import io.odpf.dagger.core.source.parquet.splitassigner.ChronologyOrderedSplitAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Supplier;

import static io.odpf.dagger.core.source.SourceName.PARQUET_SOURCE;
import static io.odpf.dagger.core.source.SourceType.BOUNDED;

public class ParquetDaggerSource implements DaggerSource<Row> {
    private final DaggerDeserializer<Row> deserializer;
    private final StreamConfig streamConfig;
    private final Configuration configuration;
    private static final SourceType SUPPORTED_SOURCE_TYPE = BOUNDED;
    private static final SourceName SUPPORTED_SOURCE_NAME = PARQUET_SOURCE;

    public ParquetDaggerSource(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.deserializer = deserializer;
    }

    @Override
    public DataStream<Row> register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy) {
        return executionEnvironment.fromSource(buildFileSource(), watermarkStrategy, streamConfig.getSchemaTable());
    }

    @Override
    public boolean canBuild() {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        if (sourceDetailsArray.length != 1) {
            return false;
        } else {
            SourceName sourceName = sourceDetailsArray[0].getSourceName();
            SourceType sourceType = sourceDetailsArray[0].getSourceType();
            return sourceName.equals(SUPPORTED_SOURCE_NAME) && sourceType.equals(SUPPORTED_SOURCE_TYPE)
                    && deserializer instanceof SimpleGroupDeserializer;
        }
    }

    FileSource<Row> buildFileSource() {
        ParquetFileSource.Builder parquetFileSourceBuilder = ParquetFileSource.Builder.getInstance();
        ParquetFileRecordFormat parquetFileRecordFormat = buildParquetFileRecordFormat();
        FileSplitAssigner.Provider splitAssignerProvider = buildParquetFileSplitAssignerProvider();
        Path[] filePaths = buildFlinkFilePaths();

        ParquetFileSource parquetFileSource = parquetFileSourceBuilder.setFilePaths(filePaths)
                .setConfiguration(configuration)
                .setFileRecordFormat(parquetFileRecordFormat)
                .setSourceType(SUPPORTED_SOURCE_TYPE)
                .setFileSplitAssigner(splitAssignerProvider)
                .build();
        return parquetFileSource.buildFileSource();
    }

    private Path[] buildFlinkFilePaths() {
        String[] parquetFilePaths = streamConfig.getParquetFilePaths();
        return Arrays.stream(parquetFilePaths)
                .map(Path::new)
                .toArray(Path[]::new);
    }

    private FileSplitAssigner.Provider buildParquetFileSplitAssignerProvider() {
        SourceParquetReadOrderStrategy readOrderStrategy = streamConfig.getParquetFilesReadOrderStrategy();
        switch (readOrderStrategy) {
            case EARLIEST_TIME_URL_FIRST:
                return ChronologyOrderedSplitAssigner::new;
            case EARLIEST_INDEX_FIRST:
            default:
                throw new DaggerConfigurationException("Error: file split assignment strategy not configured or not supported yet.");
        }
    }

    private ParquetFileRecordFormat buildParquetFileRecordFormat() {
        SimpleGroupDeserializer simpleGroupDeserializer = (SimpleGroupDeserializer) deserializer;
        ReaderProvider parquetFileReaderProvider = new ParquetReader.ParquetReaderProvider(simpleGroupDeserializer);
        ParquetFileRecordFormat.Builder parquetFileRecordFormatBuilder = ParquetFileRecordFormat.Builder.getInstance();
        Supplier<TypeInformation<Row>> typeInformationProvider = (Supplier<TypeInformation<Row>> & Serializable) simpleGroupDeserializer::getProducedType;
        return parquetFileRecordFormatBuilder
                .setParquetFileReaderProvider(parquetFileReaderProvider)
                .setTypeInformationProvider(typeInformationProvider)
                .build();
    }
}
