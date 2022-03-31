package io.odpf.dagger.core.source.parquet.reader;

import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.core.exception.ParquetFileSourceReaderInitializationException;
import lombok.Getter;
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
import java.io.Serializable;

public class PrimitiveReader implements FileRecordFormat.Reader<Row>, Serializable {
    @Getter
    private final Path hadoopFilePath;
    @Getter
    private final SimpleGroupDeserializer simpleGroupDeserializer;
    private long currentRecordIndex;
    private final ParquetFileReader reader;
    private long rowCount;
    private RecordReader<Group> recordReader;
    private final MessageType schema;
    private static final Logger LOGGER = LoggerFactory.getLogger(PrimitiveReader.class.getName());

    public PrimitiveReader(String filePath, SimpleGroupDeserializer simpleGroupDeserializer) throws IOException {
        this.hadoopFilePath = new Path(filePath);
        this.simpleGroupDeserializer = simpleGroupDeserializer;
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
        schema = reader.getFileMetaData().getSchema();
        changeReaderPosition(reader.readNextRowGroup());
        String logMessage = String.format("Successfully created the ParquetFileReader and RecordReader for file %s", hadoopFilePath.getName());
        LOGGER.info(logMessage);
    }

    private void changeReaderPosition(PageReadStore pages) {
        rowCount = pages.getRowCount();
        currentRecordIndex = 0;
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
    }

    @Nullable
    @Override
    public Row read() throws IOException {
        if (currentRecordIndex >= rowCount) {
            PageReadStore nextPage = reader.readNextRowGroup();
            if (nextPage == null) {
                return null;
            }
            changeReaderPosition(nextPage);
        }
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        currentRecordIndex++;
        return simpleGroupDeserializer.deserialize(simpleGroup);
    }

    @Override
    public void close() throws IOException {
        String logMessage = String.format("Closing the ParquetFileReader and de-referencing the RecordReader for file %s", hadoopFilePath.getName());
        LOGGER.info(logMessage);
        reader.close();
        recordReader = null;
    }

    public static class PrimitiveReaderProvider implements ReaderProvider {
        private final SimpleGroupDeserializer simpleGroupDeserializer;

        public PrimitiveReaderProvider(SimpleGroupDeserializer simpleGroupDeserializer) {
            this.simpleGroupDeserializer = simpleGroupDeserializer;
        }

        @Override
        public PrimitiveReader getReader(String filePath) {
            try {
                return new PrimitiveReader(filePath, simpleGroupDeserializer);
            } catch (IOException | RuntimeException ex) {
                throw new ParquetFileSourceReaderInitializationException(ex);
            }
        }
    }
}
