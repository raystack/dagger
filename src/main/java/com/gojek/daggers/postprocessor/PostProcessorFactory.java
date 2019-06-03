package com.gojek.daggers.postprocessor;

import com.gojek.daggers.longbow.LongBowReader;
import com.gojek.daggers.longbow.LongBowSchema;
import com.gojek.daggers.longbow.LongBowWriter;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

import static com.gojek.daggers.Constants.*;

public class PostProcessorFactory {
    public static Optional<PostProcessor> getPostProcessor(Configuration configuration, StencilClient stencilClient, String[] columnNames) {
        if (configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT))
            return Optional.of(new AshikoProcessor(configuration, stencilClient));
        if (configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT).contains(LONGBOW_KEY)) {
            final LongBowSchema longBowSchema = new LongBowSchema(columnNames);
            LongBowWriter longbowWriter = new LongBowWriter(configuration, longBowSchema);
            LongBowReader longbowReader = new LongBowReader(configuration, longBowSchema);
            return Optional.of(new LongBowProcessor(longbowWriter, longbowReader, new AsyncProcessor(), longBowSchema));
        }
        return Optional.empty();
    }
}
