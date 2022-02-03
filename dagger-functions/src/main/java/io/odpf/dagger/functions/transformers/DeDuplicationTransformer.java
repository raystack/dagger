package io.odpf.dagger.functions.transformers;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.core.Transformer;

import java.util.Arrays;
import java.util.Map;

/**
 * Allows to deduplicate data produced by the dagger.
 */
public class DeDuplicationTransformer extends RichFilterFunction<Row> implements Transformer {
    private static final String DE_DUP_STATE = "DE_DUP_STATE";
    private final int keyIndex;
    private final Integer ttlInSeconds;
    private MapState<String, Integer> mapState;

    /**
     * Instantiates a new De duplication transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param configuration           the configuration
     */
    public DeDuplicationTransformer(Map<String, Object> transformationArguments, String[] columnNames, Configuration configuration) {
        keyIndex = Arrays.asList(columnNames).indexOf(String.valueOf(transformationArguments.get("key_column")));
        ttlInSeconds = Integer.valueOf(String.valueOf(transformationArguments.get("ttl_in_seconds")));
    }

    @Override
    public StreamInfo transform(StreamInfo inputStreamInfo) {
        DataStream<Row> inputStream = inputStreamInfo.getDataStream();
        SingleOutputStreamOperator<Row> outputStream = inputStream
                .keyBy((KeySelector<Row, Object>) value -> value.getField(keyIndex))
                .filter(this);
        return new StreamInfo(outputStream, inputStreamInfo.getColumnNames());
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration internalFlinkConfig) throws Exception {
        super.open(internalFlinkConfig);
        MapStateDescriptor<String, Integer> deDupState = new MapStateDescriptor<>(DE_DUP_STATE, String.class, Integer.class);
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(ttlInSeconds))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        deDupState.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(deDupState);
    }

    @Override
    public boolean filter(Row value) throws Exception {
        String key = (String) value.getField(keyIndex);
        boolean keyAlreadyPresent = mapState.contains(key);
        if (!keyAlreadyPresent) {
            mapState.put(key, 1);
        }
        return !keyAlreadyPresent;
    }
}
