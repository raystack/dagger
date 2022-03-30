package io.odpf.dagger.core.source.parquet.reader;


import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;

public class PrimitiveReaderTest {
    @Mock
    private SimpleGroupDeserializer deserializer;

    @Test
    public void shouldCreateReadersConfiguredWithTheSameDeserializerButForDifferentFilePaths() {
        PrimitiveReader.PrimitiveReaderProvider provider = new PrimitiveReader.PrimitiveReaderProvider(deserializer);

        PrimitiveReader reader1 = provider.getReader("gs://first-file-path");
        PrimitiveReader reader2 = provider.getReader("gs://second-file-path");

        assertEquals(new Path("gs://first-file-path"), reader1.getHadoopFilePath());
        assertEquals(new Path("gs://second-file-path"), reader2.getHadoopFilePath());
        assertEquals(deserializer, reader1.getSimpleGroupDeserializer());
        assertEquals(deserializer, reader2.getSimpleGroupDeserializer());
    }
}