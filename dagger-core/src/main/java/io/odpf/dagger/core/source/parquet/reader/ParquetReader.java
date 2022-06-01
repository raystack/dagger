package io.odpf.dagger.core.source.parquet.reader;

import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.core.exception.ParquetFileSourceReaderInitializationException;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
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

public class ParquetReader implements FileRecordFormat.Reader<Row> {
    private final Path hadoopFilePath;
    private final SimpleGroupDeserializer simpleGroupDeserializer;
    private long currentRecordIndex;
    private final ParquetFileReader parquetFileReader;
    private long rowCount;
    private boolean isRecordReaderInitialized;
    private RecordReader<Group> recordReader;
    private final MessageType schema;
    private ParquetReaderState parquetReaderState;
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReader.class.getName());

    private ParquetReader(Path hadoopFilePath, SimpleGroupDeserializer simpleGroupDeserializer, ParquetFileReader parquetFileReader) {
        this(hadoopFilePath, simpleGroupDeserializer, parquetFileReader, ParquetReaderState.NOT_INITIALIZED);
    }

    private ParquetReader(Path hadoopFilePath, SimpleGroupDeserializer simpleGroupDeserializer, ParquetFileReader parquetFileReader, ParquetReaderState parquetReaderState) {
        this.hadoopFilePath = hadoopFilePath;
        this.simpleGroupDeserializer = simpleGroupDeserializer;
        this.parquetFileReader = parquetFileReader;
        this.schema = this.parquetFileReader.getFileMetaData().getSchema();
        this.isRecordReaderInitialized = false;
        this.parquetReaderState = parquetReaderState;
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
        if (currentRecordIndex >= rowCount) {
            PageReadStore nextPage = parquetFileReader.readNextRowGroup();
            if (checkIfNullPage(nextPage)) {
                return null;
            }
            changeReaderPosition(nextPage);
        }
        return readAndDeserialize();
    }

    @Nullable
    @Override
    public Row read() throws IOException {
        if (!isRecordReaderInitialized) {
            initializeRecordReader();
        }
        return readRecords();
    }

    private Row readAndDeserialize() {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        currentRecordIndex++;
        return simpleGroupDeserializer.deserialize(simpleGroup);
    }

    @Override
    public void close() throws IOException {
        parquetFileReader.close();
        closeRecordReader();
        String logMessage = String.format("Closed the ParquetFileReader and de-referenced the RecordReader for file %s", hadoopFilePath.getName());
        LOGGER.info(logMessage);
    }

    private void closeRecordReader() {
        if (isRecordReaderInitialized) {
            this.isRecordReaderInitialized = false;
        }
        recordReader = null;
    }

    public static class ParquetReaderProvider implements ReaderProvider {
        private final SimpleGroupDeserializer simpleGroupDeserializer;

        public ParquetReaderProvider(SimpleGroupDeserializer simpleGroupDeserializer) {
            this.simpleGroupDeserializer = simpleGroupDeserializer;
        }

        @Override
        public ParquetReader getReader(String filePath) {
            try {
                Configuration conf = new Configuration();
                Path hadoopFilePath = new Path(filePath);
                ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopFilePath, conf));
                return new ParquetReader(hadoopFilePath, simpleGroupDeserializer, parquetFileReader);
            } catch (IOException | RuntimeException ex) {
                throw new ParquetFileSourceReaderInitializationException(ex);
            }
        }

        @Override
        public ParquetReader getRestoredReader(String filePath, long restoredOffset, long splitOffset) {
            try {
                Configuration conf = new Configuration();
                Path hadoopFilePath = new Path(filePath);
                ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopFilePath, conf));
                return new ParquetReader(hadoopFilePath, simpleGroupDeserializer, parquetFileReader);
            } catch (IOException | RuntimeException ex) {
                throw new ParquetFileSourceReaderInitializationException(ex);
            }
        }
    }
}
