package io.odpf.dagger.core.source.parquet;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.common.serde.proto.protohandler.TypeInformationFactory;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.dagger.core.source.parquet.reader.ReaderProvider;
import io.odpf.dagger.core.source.parquet.reader.PrimitiveReaderProvider;
import io.odpf.dagger.core.source.parquet.splitassigner.ChronologyOrderedSplitAssigner;
import io.odpf.dagger.core.source.parquet.splitassigner.IndexOrderedSplitAssigner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.function.Supplier;

public class ParquetFileSourceFactory {
    public static ParquetFileSource getFileSource(SourceType sourceType,
                                                StreamConfig streamConfig,
                                                Configuration configuration,
                                                SimpleGroupDeserializer simpleGroupDeserializer) {
        Path[] filePaths = Arrays.stream(streamConfig.getParquetFilePaths())
                .map(Path::new)
                .toArray(Path[]::new);
        ParquetFileSourceBuilder parquetFileSourceBuilder = ParquetFileSourceBuilder.getInstance();
        FileRecordFormat<Row> fileRecordFormat = getParquetFileRecordFormat(simpleGroupDeserializer);
        FileSplitAssigner.Provider splitAssignerProvider = getSplitAssignerProvider(streamConfig);

        return parquetFileSourceBuilder.setFilePaths(filePaths)
                .setConfiguration(configuration)
                .setFileRecordFormat(fileRecordFormat)
                .setSourceType(sourceType)
                .setFileSplitAssigner(splitAssignerProvider)
                .build();
    }

    private static FileRecordFormat<Row> getParquetFileRecordFormat(SimpleGroupDeserializer simpleGroupDeserializer) {
        ReaderProvider parquetFileReaderProvider = new PrimitiveReaderProvider(simpleGroupDeserializer);
        Supplier<TypeInformation<Row>> typeInformationProvider = simpleGroupDeserializer::getProducedType;
        return new ParquetFileRecordFormat(parquetFileReaderProvider, typeInformationProvider);
    }

    private static FileSplitAssigner.Provider getSplitAssignerProvider(StreamConfig streamConfig) {
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
}