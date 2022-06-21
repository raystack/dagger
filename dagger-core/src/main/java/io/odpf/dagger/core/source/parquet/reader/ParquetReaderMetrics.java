package io.odpf.dagger.core.source.parquet.reader;

import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.ParquetReaderAspects;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class ParquetReaderMetrics extends AbstractRichFunction {
    private MeterStatsManager meterStatsManager;
    private final LinkedList<ParquetReaderAspects> metricsToPublishQueue;
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReaderMetrics.class.getName());

    public ParquetReaderMetrics() {
        metricsToPublishQueue = new LinkedList<>();
        LOGGER.info("Initialized ParquetReaderMetrics class.");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOGGER.info("Lifecycle function open called in ParquetReaderMetrics class.");
        if (meterStatsManager == null) {
            LOGGER.info("MeterStatsManager is null. Creating one.");
            meterStatsManager = new MeterStatsManager(getRuntimeContext().getMetricGroup(), true);
            LOGGER.info("Calling flushAll from within open in ParquetReaderMetrics class.");
            flushAll();
        }
    }

    public void enqueueMetric(ParquetReaderAspects aspect) {
        LOGGER.info("Adding metric to queue:" + aspect.getValue());
        metricsToPublishQueue.addLast(aspect);
        if (meterStatsManager != null) {
            LOGGER.info("MeterStatsManager is not null. Flushing metric after enqueuing:" + aspect.getValue());
            flushAll();
        }
    }

    public void flushAll() {
        LOGGER.info("Preparing to flush metrics from queue.");
        while (!metricsToPublishQueue.isEmpty()) {
            ParquetReaderAspects aspect = metricsToPublishQueue.poll();
            System.out.println("Publishing metric " + aspect.getValue());
            meterStatsManager.markEvent(aspect);
        }
        LOGGER.info("All metrics in queue flushed.");
    }
}
