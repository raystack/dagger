package io.odpf.dagger.common.metrics.type.statsd;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import io.odpf.dagger.common.metrics.type.statsd.tags.StatsDTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class DaggerStatsDClient implements SerializedStatsDClientSupplier {
    private final int port;
    private final String hostname;
    private final String[] constantTags;
    private static StatsDClient statsDClient;
    private static final String PREFIX = "statsd";
    private static final Logger LOGGER = LoggerFactory.getLogger(DaggerStatsDClient.class.getName());

    public DaggerStatsDClient(String hostname, int port, StatsDTag[] globalTags) {
        this.hostname = hostname;
        this.port = port;
        this.constantTags = Arrays.stream(globalTags)
                .map(StatsDTag::getFormattedTag)
                .toArray(String[]::new);
    }

    @Override
    public StatsDClient getClient() {
        if (statsDClient == null) {
            statsDClient = new NonBlockingStatsDClientBuilder()
                    .prefix(PREFIX)
                    .hostname(hostname)
                    .port(port)
                    .constantTags(constantTags)
                    .build();
            String message = String.format("Created new instance of StatsDClient for host=%s and port=%d", hostname, port);
            LOGGER.info(message);
        }
        return statsDClient;
    }
}
