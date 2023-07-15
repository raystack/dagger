package org.raystack.dagger.functions.udfs.factories;

import org.raystack.dagger.functions.common.Constants;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.google.gson.Gson;
import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.udfs.AggregateUdf;
import org.raystack.dagger.common.udfs.ScalarUdf;
import org.raystack.dagger.common.udfs.TableUdf;
import org.raystack.dagger.common.udfs.UdfFactory;
import org.raystack.dagger.functions.udfs.aggregate.CollectArray;
import org.raystack.dagger.functions.udfs.aggregate.DistinctCount;
import org.raystack.dagger.functions.udfs.aggregate.Features;
import org.raystack.dagger.functions.udfs.aggregate.FeaturesWithType;
import org.raystack.dagger.functions.udfs.aggregate.PercentileAggregator;
import org.raystack.dagger.functions.udfs.scalar.ArrayAggregate;
import org.raystack.dagger.functions.udfs.scalar.ArrayOperate;
import org.raystack.dagger.functions.udfs.scalar.ByteToString;
import org.raystack.dagger.functions.udfs.scalar.CondEq;
import org.raystack.dagger.functions.udfs.scalar.DartContains;
import org.raystack.dagger.functions.udfs.scalar.DartGet;
import org.raystack.dagger.functions.udfs.scalar.Distance;
import org.raystack.dagger.functions.udfs.scalar.ElementAt;
import org.raystack.dagger.functions.udfs.scalar.EndOfMonth;
import org.raystack.dagger.functions.udfs.scalar.EndOfWeek;
import org.raystack.dagger.functions.udfs.scalar.ExponentialMovingAverage;
import org.raystack.dagger.functions.udfs.scalar.Filters;
import org.raystack.dagger.functions.udfs.scalar.FormatTimeInZone;
import org.raystack.dagger.functions.udfs.scalar.GeoHash;
import org.raystack.dagger.functions.udfs.scalar.LinearTrend;
import org.raystack.dagger.functions.udfs.scalar.ListContains;
import org.raystack.dagger.functions.udfs.scalar.MapGet;
import org.raystack.dagger.functions.udfs.scalar.S2AreaInKm2;
import org.raystack.dagger.functions.udfs.scalar.S2Id;
import org.raystack.dagger.functions.udfs.scalar.SelectFields;
import org.raystack.dagger.functions.udfs.scalar.SingleFeatureWithType;
import org.raystack.dagger.functions.udfs.scalar.Split;
import org.raystack.dagger.functions.udfs.scalar.StartOfMonth;
import org.raystack.dagger.functions.udfs.scalar.StartOfWeek;
import org.raystack.dagger.functions.udfs.scalar.TimeInDate;
import org.raystack.dagger.functions.udfs.scalar.TimestampFromUnix;
import org.raystack.dagger.functions.udfs.scalar.JsonQuery;
import org.raystack.dagger.functions.udfs.scalar.JsonUpdate;
import org.raystack.dagger.functions.udfs.scalar.JsonDelete;
import org.raystack.dagger.functions.udfs.table.HistogramBucket;
import org.raystack.dagger.functions.udfs.table.OutlierMad;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.raystack.dagger.common.core.Constants.INPUT_STREAMS;
import static org.raystack.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_PROTO_CLASS;
import static org.raystack.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_TABLE;

/**
 * The factory class for all the udf.
 */
public class FunctionFactory extends UdfFactory {

    private static final Gson GSON = new Gson();

    private final StencilClientOrchestrator stencilClientOrchestrator;


    /**
     * Instantiates a new Function factory.
     *
     * @param streamTableEnvironment the stream table environment
     * @param configuration          the configuration
     */
    public FunctionFactory(StreamTableEnvironment streamTableEnvironment, Configuration configuration) {
        super(streamTableEnvironment, configuration);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }

    @Override
    public HashSet<ScalarUdf> getScalarUdfs() {
        HashSet<ScalarUdf> scalarUdfs = new HashSet<>();
        scalarUdfs.add(DartContains.withGcsDataStore(getGcsProjectId(), getGcsBucketId()));
        scalarUdfs.add(DartGet.withGcsDataStore(getGcsProjectId(), getGcsBucketId()));
        scalarUdfs.add(new Distance());
        scalarUdfs.add(new ElementAt(getProtosInInputStreams(), stencilClientOrchestrator));
        scalarUdfs.add(new EndOfMonth());
        scalarUdfs.add(new EndOfWeek());
        scalarUdfs.add(new ExponentialMovingAverage());
        scalarUdfs.add(new FormatTimeInZone());
        scalarUdfs.add(new GeoHash());
        scalarUdfs.add(new LinearTrend());
        scalarUdfs.add(new ListContains());
        scalarUdfs.add(new MapGet());
        scalarUdfs.add(new S2AreaInKm2());
        scalarUdfs.add(new S2Id());
        scalarUdfs.add(new SingleFeatureWithType());
        scalarUdfs.add(new Split());
        scalarUdfs.add(new StartOfMonth());
        scalarUdfs.add(new StartOfWeek());
        scalarUdfs.add(new TimeInDate());
        scalarUdfs.add(new TimestampFromUnix());
        scalarUdfs.add(new CondEq());
        scalarUdfs.add(new Filters(stencilClientOrchestrator));
        scalarUdfs.add(new SelectFields(stencilClientOrchestrator));
        scalarUdfs.add(new ArrayAggregate());
        scalarUdfs.add(new ArrayOperate());
        scalarUdfs.add(new ByteToString());
        scalarUdfs.add(new JsonQuery());
        scalarUdfs.add(new JsonUpdate());
        scalarUdfs.add(new JsonDelete());
        return scalarUdfs;
    }

    @Override
    public HashSet<TableUdf> getTableUdfs() {
        HashSet<TableUdf> tableUdfs = new HashSet<>();
        tableUdfs.add(new HistogramBucket());
        tableUdfs.add(new OutlierMad());
        return tableUdfs;
    }

    @Override
    public HashSet<AggregateUdf> getAggregateUdfs() {
        HashSet<AggregateUdf> aggregateUdfs = new HashSet<>();
        aggregateUdfs.add(new CollectArray());
        aggregateUdfs.add(new DistinctCount());
        aggregateUdfs.add(new Features());
        aggregateUdfs.add(new FeaturesWithType());
        aggregateUdfs.add(new PercentileAggregator());
        return aggregateUdfs;
    }

    private String getGcsProjectId() {
        return getConfiguration().getString(Constants.UDF_DART_GCS_PROJECT_ID_KEY, Constants.UDF_DART_GCS_PROJECT_ID_DEFAULT);
    }

    private String getGcsBucketId() {
        return getConfiguration().getString(Constants.UDF_DART_GCS_BUCKET_ID_KEY, Constants.UDF_DART_GCS_BUCKET_ID_DEFAULT);
    }

    private LinkedHashMap<String, String> getProtosInInputStreams() {
        LinkedHashMap<String, String> protoClassForTable = new LinkedHashMap<>();
        String jsonArrayString = getConfiguration().getString(INPUT_STREAMS, "");
        Map[] streamsConfig = GSON.fromJson(jsonArrayString, Map[].class);
        for (Map<String, String> streamConfig : streamsConfig) {
            String protoClassName = streamConfig.getOrDefault(STREAM_INPUT_SCHEMA_PROTO_CLASS, "");
            String tableName = streamConfig.getOrDefault(STREAM_INPUT_SCHEMA_TABLE, "");
            protoClassForTable.put(tableName, protoClassName);
        }
        return protoClassForTable;
    }
}
