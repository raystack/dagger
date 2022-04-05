package io.odpf.dagger.core.streamtype;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.dagger.core.source.parquet.ParquetFileRecordFormat;
import io.odpf.dagger.core.source.parquet.ParquetFileSource;
import io.odpf.dagger.core.source.parquet.SourceParquetReadOrderStrategy;
import io.odpf.dagger.core.source.parquet.reader.PrimitiveReader;
import io.odpf.dagger.core.source.parquet.reader.ReaderProvider;
import io.odpf.dagger.core.source.parquet.splitassigner.ChronologyOrderedSplitAssigner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Supplier;

import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_KEY;

public class ParquetSourceProtoSchemaStreamType extends StreamType<Row> {

    public ParquetSourceProtoSchemaStreamType(Source source, String streamName, DataTypes inputDataType, DaggerDeserializer<Row> deserializer) {
        super(source, streamName, inputDataType, deserializer);
    }

    public static class ParquetSourceProtoSchemaStreamTypeBuilder extends StreamType.Builder<Row> {
        protected static final DataTypes SUPPORTED_INPUT_DATA_TYPE = DataTypes.PROTO;
        protected static final SourceName SUPPORTED_SOURCE_NAME = SourceName.PARQUET;
        protected static final SourceType SUPPORTED_SOURCE_TYPE = SourceType.BOUNDED;

        public ParquetSourceProtoSchemaStreamTypeBuilder(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
            super(streamConfig, configuration, stencilClientOrchestrator);
        }

        @Override
        public boolean canBuild() {
            SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
            if (sourceDetailsArray.length != 1) {
                return false;
            } else {
                SourceName sourceName = sourceDetailsArray[0].getSourceName();
                SourceType sourceType = sourceDetailsArray[0].getSourceType();
                DataTypes inputDataType = DataTypes.valueOf(streamConfig.getDataType());
                return sourceName.equals(SUPPORTED_SOURCE_NAME)
                        && sourceType.equals(SUPPORTED_SOURCE_TYPE)
                        && inputDataType.equals(SUPPORTED_INPUT_DATA_TYPE);
            }
        }

        @Override
        public StreamType<Row> build() {
            SimpleGroupDeserializer deserializer = buildSimpleGroupDeserializer();
            Source source = buildFileSource(deserializer);
            String streamName = streamConfig.getSchemaTable();
            return new ParquetSourceProtoSchemaStreamType(source, streamName, SUPPORTED_INPUT_DATA_TYPE, deserializer);
        }

        private SimpleGroupDeserializer buildSimpleGroupDeserializer() {
            int timestampFieldIndex = Integer.parseInt(streamConfig.getEventTimestampFieldIndex());
            String protoClassName = streamConfig.getProtoClass();
            String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
            return new SimpleGroupDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClientOrchestrator);
        }

        private FileSource<Row> buildFileSource(SimpleGroupDeserializer simpleGroupDeserializer) {
            ParquetFileSource.Builder parquetFileSourceBuilder = ParquetFileSource.Builder.getInstance();
            ParquetFileRecordFormat parquetFileRecordFormat = buildParquetFileRecordFormat(simpleGroupDeserializer);
            FileSplitAssigner.Provider splitAssignerProvider = buildParquetFileSplitAssignerProvider(streamConfig);
            Path[] filePaths = buildFlinkFilePaths(streamConfig);

            ParquetFileSource parquetFileSource = parquetFileSourceBuilder.setFilePaths(filePaths)
                    .setConfiguration(configuration)
                    .setFileRecordFormat(parquetFileRecordFormat)
                    .setSourceType(SUPPORTED_SOURCE_TYPE)
                    .setFileSplitAssigner(splitAssignerProvider)
                    .build();
            return parquetFileSource.buildFileSource();
        }

        private Path[] buildFlinkFilePaths(StreamConfig streamConfig) {
            String[] parquetFilePaths = streamConfig.getParquetFilePaths();
            return Arrays.stream(parquetFilePaths)
                    .map(Path::new)
                    .toArray(Path[]::new);
        }

        private FileSplitAssigner.Provider buildParquetFileSplitAssignerProvider(StreamConfig streamConfig) {
            SourceParquetReadOrderStrategy readOrderStrategy = streamConfig.getParquetFilesReadOrderStrategy();
            switch (readOrderStrategy) {
                case EARLIEST_TIME_URL_FIRST:
                    return ChronologyOrderedSplitAssigner::new;
                case EARLIEST_INDEX_FIRST:
                default:
                    throw new DaggerConfigurationException("Error: file split assignment strategy not configured or not supported yet.");
            }
        }

        private ParquetFileRecordFormat buildParquetFileRecordFormat(SimpleGroupDeserializer simpleGroupDeserializer) {
            ReaderProvider parquetFileReaderProvider = new PrimitiveReader.PrimitiveReaderProvider(simpleGroupDeserializer);
            ParquetFileRecordFormat.Builder parquetFileRecordFormatBuilder = ParquetFileRecordFormat.Builder.getInstance();
            Supplier<TypeInformation<Row>> typeInformationProvider = (Supplier<TypeInformation<Row>> & Serializable) simpleGroupDeserializer::getProducedType;
            return parquetFileRecordFormatBuilder
                    .setParquetFileReaderProvider(parquetFileReaderProvider)
                    .setTypeInformationProvider(typeInformationProvider)
                    .build();
        }
    }
}
