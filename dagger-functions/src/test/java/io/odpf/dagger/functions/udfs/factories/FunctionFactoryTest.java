package io.odpf.dagger.functions.udfs.factories;

import io.odpf.dagger.common.udfs.AggregateUdf;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.common.udfs.TableUdf;
import io.odpf.dagger.functions.udfs.aggregate.CollectArray;
import io.odpf.dagger.functions.udfs.aggregate.DistinctCount;
import io.odpf.dagger.functions.udfs.aggregate.Features;
import io.odpf.dagger.functions.udfs.aggregate.FeaturesWithType;
import io.odpf.dagger.functions.udfs.aggregate.PercentileAggregator;
import io.odpf.dagger.functions.udfs.scalar.DartContains;
import io.odpf.dagger.functions.udfs.scalar.DartGet;
import io.odpf.dagger.functions.udfs.scalar.Distance;
import io.odpf.dagger.functions.udfs.scalar.ElementAt;
import io.odpf.dagger.functions.udfs.scalar.EndOfMonth;
import io.odpf.dagger.functions.udfs.scalar.EndOfWeek;
import io.odpf.dagger.functions.udfs.scalar.ExponentialMovingAverage;
import io.odpf.dagger.functions.udfs.scalar.FormatTimeInZone;
import io.odpf.dagger.functions.udfs.scalar.GeoHash;
import io.odpf.dagger.functions.udfs.scalar.LinearTrend;
import io.odpf.dagger.functions.udfs.scalar.ListContains;
import io.odpf.dagger.functions.udfs.scalar.MapGet;
import io.odpf.dagger.functions.udfs.scalar.S2AreaInKm2;
import io.odpf.dagger.functions.udfs.scalar.S2Id;
import io.odpf.dagger.functions.udfs.scalar.SingleFeatureWithType;
import io.odpf.dagger.functions.udfs.scalar.Split;
import io.odpf.dagger.functions.udfs.scalar.StartOfMonth;
import io.odpf.dagger.functions.udfs.scalar.StartOfWeek;
import io.odpf.dagger.functions.udfs.scalar.TimeInDate;
import io.odpf.dagger.functions.udfs.scalar.TimestampFromUnix;
import io.odpf.dagger.functions.udfs.table.HistogramBucket;
import io.odpf.dagger.functions.udfs.table.OutlierMad;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashSet;

import static io.odpf.dagger.common.core.Constants.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FunctionFactoryTest {

    @Mock
    private StreamTableEnvironment streamTableEnvironment;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        String jsonArray = "[\n"
                + "        {\n"
                + "            \"EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"io.odpf.test.TestLogMessage\",\n"
                + "            \"TABLE_NAME\": \"data_stream\",\n"
                + "            \"TOPIC_NAMES\": \"test-log\"\n"
                + "        }\n"
                + "]";
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn(jsonArray);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, false)).thenReturn(false);
        when(configuration.getString(STENCIL_URL_KEY, "")).thenReturn("");
    }

    @Test
    public void shouldReturnScalarUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<ScalarUdf> scalarUdfs = functionFactory.getScalarUdfs();
        Assert.assertTrue(scalarUdfs.stream().anyMatch(scalarUdf -> scalarUdf.getClass() == EndOfMonth.class));
    }

    @Test
    public void shouldRegisterScalarUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        functionFactory.registerFunctions();
        verify(streamTableEnvironment, times(1)).registerFunction(eq("DartContains"), any(DartContains.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("DartGet"), any(DartGet.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("Distance"), any(Distance.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("ElementAt"), any(ElementAt.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("EndOfMonth"), any(EndOfMonth.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("EndOfWeek"), any(EndOfWeek.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("ExponentialMovingAverage"), any(ExponentialMovingAverage.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("FormatTimeInZone"), any(FormatTimeInZone.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("GeoHash"), any(GeoHash.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("LinearTrend"), any(LinearTrend.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("ListContains"), any(ListContains.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("MapGet"), any(MapGet.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("S2AreaInKm2"), any(S2AreaInKm2.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("S2Id"), any(S2Id.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("SingleFeatureWithType"), any(SingleFeatureWithType.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("Split"), any(Split.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("StartOfMonth"), any(StartOfMonth.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("StartOfWeek"), any(StartOfWeek.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("TimeInDate"), any(TimeInDate.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("TimestampFromUnix"), any(TimestampFromUnix.class));
    }

    @Test
    public void shouldReturnTableUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<TableUdf> tableUdfs = functionFactory.getTableUdfs();
        Assert.assertTrue(tableUdfs.stream().anyMatch(tableUdf -> tableUdf.getClass() == HistogramBucket.class));
    }

    @Test
    public void shouldRegisterTableUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        functionFactory.registerFunctions();
        verify(streamTableEnvironment, times(1)).registerFunction(eq("OutlierMad"), any(OutlierMad.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("HistogramBucket"), any(HistogramBucket.class));
    }

    @Test
    public void shouldReturnAggregateUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<AggregateUdf> aggregateUdfs = functionFactory.getAggregateUdfs();
        Assert.assertTrue(aggregateUdfs.stream().anyMatch(aggregateUdf -> aggregateUdf.getClass() == DistinctCount.class));
    }

    @Test
    public void shouldRegisterAggregateUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        functionFactory.registerFunctions();
        verify(streamTableEnvironment, times(1)).registerFunction(eq("CollectArray"), any(CollectArray.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("DistinctCount"), any(DistinctCount.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("Features"), any(Features.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("FeaturesWithType"), any(FeaturesWithType.class));
        verify(streamTableEnvironment, times(1)).registerFunction(eq("PercentileAggregator"), any(PercentileAggregator.class));
    }

}
