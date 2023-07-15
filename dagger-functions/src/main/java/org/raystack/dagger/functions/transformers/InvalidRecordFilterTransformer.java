package org.raystack.dagger.functions.transformers;

import org.raystack.dagger.common.core.DaggerContext;
import org.raystack.dagger.functions.transformers.filter.FilterAspects;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;

import org.raystack.dagger.common.core.StreamInfo;
import org.raystack.dagger.common.core.Transformer;
import org.raystack.dagger.common.metrics.managers.CounterStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;


/**
 * Filter the invalid records produced by dagger.
 */
public class InvalidRecordFilterTransformer extends RichFilterFunction<Row> implements Transformer {
    private final String tableName;
    private final int validationIndex;
    private CounterStatsManager metricsManager = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(InvalidRecordFilterTransformer.class.getName());

    protected static final String INTERNAL_VALIDATION_FILED = "__internal_validation_field__";
    private static final String PER_TABLE = "per_table";

    /**
     * Instantiates a new Invalid record filter transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param daggerContext           the daggerContext
     */
    public InvalidRecordFilterTransformer(Map<String, Object> transformationArguments, String[] columnNames, DaggerContext daggerContext) {
        this.tableName = (String) transformationArguments.getOrDefault("table_name", "");
        validationIndex = Arrays.asList(columnNames).indexOf(INTERNAL_VALIDATION_FILED);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration internalFlinkConfig) throws Exception {
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        metricsManager = new CounterStatsManager(metricGroup);
        metricsManager.register(FilterAspects.FILTERED_INVALID_RECORDS, PER_TABLE, tableName);
    }

    @Override
    public boolean filter(Row value) {
        if (!(boolean) value.getField(validationIndex)) {
            metricsManager.inc(FilterAspects.FILTERED_INVALID_RECORDS);
            LOGGER.info("Filtering invalid record for table "
                    + this.tableName + "\n"
                    + "Total = ", metricsManager.getCount(FilterAspects.FILTERED_INVALID_RECORDS));
            return false;
        }
        return true;
    }

    @Override
    public StreamInfo transform(StreamInfo streamInfo) {
        return new StreamInfo(
                streamInfo.getDataStream().filter(this),
                streamInfo.getColumnNames());
    }
}
