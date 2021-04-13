package io.odpf.dagger.metrics.telemetry;

import com.gojek.dagger.udf.DistinctCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.AggregateFunction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static io.odpf.dagger.utils.Constants.SQL_QUERY;
import static io.odpf.dagger.utils.Constants.SQL_QUERY_DEFAULT;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class AggregatedUDFTelemetryPublisherTest {

    @Mock
    private Configuration configuration;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldRegisterAggregatedUDFs() {
        ArrayList<String> aggregatedFunctionNames = new ArrayList<>();
        aggregatedFunctionNames.add("DistinctCount");
        HashMap<String, List<String>> expectedMetrics = new HashMap<>();
        expectedMetrics.put("udf", aggregatedFunctionNames);

        HashMap<String, AggregateFunction> aggregateFunctionHashMap = new HashMap<>();
        aggregateFunctionHashMap.put("DistinctCount", new DistinctCount());
        AggregatedUDFTelemetryPublisher aggregatedUDFTelemetryPublisher = new AggregatedUDFTelemetryPublisher(configuration, aggregateFunctionHashMap);
        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("SELECT DistinctCount(driver_id) AS " +
                "distinctCountDrivers, TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp from data_stream_0" +
                " GROUP BY TUMBLE (rowtime, INTERVAL '60' SECOND)");
        aggregatedUDFTelemetryPublisher.preProcessBeforeNotifyingSubscriber();

        Assert.assertEquals(expectedMetrics, aggregatedUDFTelemetryPublisher.getTelemetry());
    }

    @Test
    public void shouldNotRegisterNonAggregatedUDFs() {
        HashMap<String, AggregateFunction> aggregateFunctionHashMap = new HashMap<>();
        aggregateFunctionHashMap.put("DistinctCount", new DistinctCount());
        AggregatedUDFTelemetryPublisher aggregatedUDFTelemetryPublisher = new AggregatedUDFTelemetryPublisher(configuration, aggregateFunctionHashMap);
        when(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT)).thenReturn("SELECT * FROM `booking`");
        aggregatedUDFTelemetryPublisher.preProcessBeforeNotifyingSubscriber();

        Assert.assertEquals(new HashMap<>(), aggregatedUDFTelemetryPublisher.getTelemetry());
    }
}