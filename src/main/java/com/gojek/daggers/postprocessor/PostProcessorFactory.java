package com.gojek.daggers.postprocessor;

import com.gojek.daggers.longbow.row.LongbowRowFactory;
import com.gojek.daggers.longbow.processor.LongbowReader;
import com.gojek.daggers.longbow.LongbowSchema;
import com.gojek.daggers.longbow.processor.LongbowWriter;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

import static com.gojek.daggers.Constants.*;

public class PostProcessorFactory {
    public static Optional<PostProcessor> getPostProcessor(Configuration configuration, StencilClient stencilClient, String[] columnNames) {
        if (configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT))
            return Optional.of(new AshikoProcessor(configuration, stencilClient));
        if (configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT).contains(LONGBOW_KEY)) {
            final LongbowSchema longbowSchema = new LongbowSchema(columnNames);
            LongbowWriter longbowWriter = new LongbowWriter(configuration, longbowSchema);
            LongbowReader longbowReader = new LongbowReader(configuration, longbowSchema, LongbowRowFactory.getLongbowRow(longbowSchema));
            return Optional.of(new LongbowProcessor(longbowWriter, longbowReader, new AsyncProcessor(), longbowSchema, configuration));
        }
        if (configuration.getBoolean(EXTERNAL_SOURCE_ENABLED_KEY, EXTERNAL_SOURCE_ENABLED_KEY_DEFAULT))
            return Optional.of(new ExternalSourceProcessor(configuration, stencilClient));
        return Optional.empty();
    }
}
