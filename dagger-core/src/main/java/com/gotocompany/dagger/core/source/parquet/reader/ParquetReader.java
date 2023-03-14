package com.gotocompany.dagger.core.source.parquet.reader;

import com.gotocompany.dagger.common.exceptions.serde.DaggerDeserializationException;
import com.gotocompany.dagger.core.exception.ParquetFileSourceReaderInitializationException;
import com.gotocompany.dagger.core.metrics.aspects.ParquetReaderAspects;
import com.gotocompany.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import com.gotocompany.dagger.core.metrics.reporters.statsd.StatsDErrorReporter;
import com.gotocompany.dagger.core.metrics.reporters.statsd.manager.DaggerCounterManager;
import com.gotocompany.dagger.core.metrics.reporters.statsd.manager.DaggerHistogramManager;
import com.gotocompany.dagger.core.metrics.reporters.statsd.tags.ComponentTags;
import com.gotocompany.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import com.gotocompany.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;

public class ParquetReader implements FileRecordFormat.Reader<Row> {
    private final Path hadoopFilePath;
    private final SimpleGroupDeserializer simpleGroupDeserializer;
    private long currentRecordIndex;
    private final ParquetFileReader parquetFileReader;
    private long rowCount;
    private boolean isRecordReaderInitialized;
    private RecordReader<Group> recordReader;
    private final MessageType schema;
    private long totalEmittedRowCount;
    private DaggerCounterManager daggerCounterManager;
    private DaggerHistogramManager daggerHistogramManager;
    private final StatsDErrorReporter statsDErrorReporter;
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReader.class.getName());

    private ParquetReader(Path hadoopFilePath, SimpleGroupDeserializer simpleGroupDeserializer, ParquetFileReader
            parquetFileReader, SerializedStatsDReporterSupplier statsDReporterSupplier) throws IOException {
        this.hadoopFilePath = hadoopFilePath;
        this.simpleGroupDeserializer = simpleGroupDeserializer;
        this.parquetFileReader = parquetFileReader;
        this.schema = this.parquetFileReader.getFileMetaData().getSchema();
        this.isRecordReaderInitialized = false;
        this.totalEmittedRowCount = 0L;
        this.registerTagsWithMeasurementManagers(statsDReporterSupplier);
        this.statsDErrorReporter = new StatsDErrorReporter(statsDReporterSupplier);
        daggerCounterManager.increment(ParquetReaderAspects.READER_CREATED);
    }

    private void registerTagsWithMeasurementManagers(SerializedStatsDReporterSupplier statsDReporterSupplier) {
        StatsDTag[] parquetReaderTags = ComponentTags.getParquetReaderTags();
        this.daggerCounterManager = new DaggerCounterManager(statsDReporterSupplier);
        this.daggerCounterManager.register(parquetReaderTags);
        this.daggerHistogramManager = new DaggerHistogramManager(statsDReporterSupplier);
        this.daggerHistogramManager.register(parquetReaderTags);
    }

    private boolean checkIfNullPage(PageReadStore page) {
        if (page == null) {
            String logMessage = String.format("No more data found in Parquet file %s", hadoopFilePath.getName());
            LOGGER.info(logMessage);
            return true;
        }
        return false;
    }

    private void changeReaderPosition(PageReadStore pages) {
        rowCount = pages.getRowCount();
        currentRecordIndex = 0;
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
    }

    private void initializeRecordReader() throws IOException {
        PageReadStore nextPage = parquetFileReader.readNextRowGroup();
        changeReaderPosition(nextPage);
        this.isRecordReaderInitialized = true;
        String logMessage = String.format("Successfully created the ParquetFileReader and RecordReader for file %s", hadoopFilePath.getName());
        LOGGER.info(logMessage);
    }

    private Row readRecords() throws IOException {
        long startReadTime = Instant.now().toEpochMilli();

        if (currentRecordIndex >= rowCount) {
            PageReadStore nextPage = parquetFileReader.readNextRowGroup();
            if (checkIfNullPage(nextPage)) {
                return null;
            }
            changeReaderPosition(nextPage);
        }
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        long endReadTime = Instant.now().toEpochMilli();

        currentRecordIndex++;
        long startDeserializationTime = Instant.now().toEpochMilli();

        Row row = deserialize(simpleGroup);

        long endDeserializationTime = Instant.now().toEpochMilli();
        totalEmittedRowCount++;

        daggerHistogramManager.recordValue(ParquetReaderAspects.READER_ROW_READ_TIME, endReadTime - startReadTime);
        daggerHistogramManager.recordValue(ParquetReaderAspects.READER_ROW_DESERIALIZATION_TIME, endDeserializationTime - startDeserializationTime);
        return row;
    }

    private Row deserialize(SimpleGroup simpleGroup) {
        try {
            return simpleGroupDeserializer.deserialize(simpleGroup);
        } catch (DaggerDeserializationException exception) {
            statsDErrorReporter.reportFatalException(exception);
            throw exception;
        }
    }

    @Nullable
    @Override
    public Row read() throws IOException {
        if (!isRecordReaderInitialized) {
            initializeRecordReader();
        }
        Row row = readRecords();
        daggerCounterManager.increment(ParquetReaderAspects.READER_ROWS_EMITTED);
        return row;
    }

    @Override
    public void close() throws IOException {
        parquetFileReader.close();
        closeRecordReader();
        String logMessage = String.format("Closed the ParquetFileReader and de-referenced the RecordReader for file %s", hadoopFilePath.getName());
        LOGGER.info(logMessage);
        daggerCounterManager.increment(ParquetReaderAspects.READER_CLOSED);
    }

    private void closeRecordReader() {
        if (isRecordReaderInitialized) {
            this.isRecordReaderInitialized = false;
        }
        recordReader = null;
    }

    @Override
    public CheckpointedPosition getCheckpointedPosition() {
        return new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, totalEmittedRowCount);
    }

    public static class ParquetReaderProvider implements ReaderProvider {
        private final SimpleGroupDeserializer simpleGroupDeserializer;
        private final SerializedStatsDReporterSupplier statsDReporterSupplier;

        public ParquetReaderProvider(SimpleGroupDeserializer simpleGroupDeserializer, SerializedStatsDReporterSupplier statsDReporterSupplier) {
            this.simpleGroupDeserializer = simpleGroupDeserializer;
            this.statsDReporterSupplier = statsDReporterSupplier;
        }

        @Override
        public ParquetReader getReader(String filePath) {
            try {
                Configuration conf = new Configuration();
                Path hadoopFilePath = new Path(filePath);
                ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopFilePath, conf));
                return new ParquetReader(hadoopFilePath, simpleGroupDeserializer, parquetFileReader, statsDReporterSupplier);
            } catch (IOException | RuntimeException ex) {
                ParquetFileSourceReaderInitializationException exception = new ParquetFileSourceReaderInitializationException(ex);
                new StatsDErrorReporter(statsDReporterSupplier).reportFatalException(exception);
                throw exception;
            }
        }
    }
}
