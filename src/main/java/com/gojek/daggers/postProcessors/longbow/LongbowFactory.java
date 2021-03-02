package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.columnModifier.LongbowReadColumnModifier;
import com.gojek.daggers.postProcessors.longbow.columnModifier.LongbowWriteColumnModifier;
import com.gojek.daggers.postProcessors.longbow.columnModifier.NoOpColumnModifier;
import com.gojek.daggers.postProcessors.longbow.data.LongbowProtoData;
import com.gojek.daggers.postProcessors.longbow.data.LongbowTableData;
import com.gojek.daggers.postProcessors.longbow.outputRow.OutputIdentity;
import com.gojek.daggers.postProcessors.longbow.outputRow.OutputSynchronizer;
import com.gojek.daggers.postProcessors.longbow.outputRow.ReaderOutputLongbowData;
import com.gojek.daggers.postProcessors.longbow.outputRow.ReaderOutputProtoData;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowReader;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowWriter;
import com.gojek.daggers.postProcessors.longbow.request.PutRequestFactory;
import com.gojek.daggers.postProcessors.longbow.request.ScanRequestFactory;
import com.gojek.daggers.postProcessors.longbow.range.LongbowRange;
import com.gojek.daggers.postProcessors.longbow.range.LongbowRangeFactory;
import com.gojek.daggers.postProcessors.longbow.validator.LongbowType;
import com.gojek.daggers.postProcessors.longbow.validator.LongbowValidator;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.sink.ProtoSerializer;
import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowFactory {
    private LongbowSchema longbowSchema;
    private AsyncProcessor asyncProcessor;
    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private MetricsTelemetryExporter metricsTelemetryExporter;
    private String[] columnNames;
    private static final Gson gson = new Gson();

    public LongbowFactory(LongbowSchema longbowSchema, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, MetricsTelemetryExporter metricsTelemetryExporter) {
        this.longbowSchema = longbowSchema;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.columnNames = longbowSchema.getColumnNames().toArray(new String[0]);
        this.asyncProcessor = new AsyncProcessor();
    }

    public LongbowFactory(LongbowSchema longbowSchema, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, MetricsTelemetryExporter metricsTelemetryExporter, AsyncProcessor asyncProcessor) {
        this(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter);
        this.longbowSchema = longbowSchema;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.columnNames = longbowSchema.getColumnNames().toArray(new String[0]);
        this.asyncProcessor = asyncProcessor;
    }

    public PostProcessor getLongbowProcessor() {
        LongbowReader longbowReader;
        LongbowWriter longbowWriter;
        LongbowValidator longbowValidator = new LongbowValidator(columnNames);
        LongbowType longbowType = longbowSchema.getType();

        ArrayList<RichAsyncFunction<Row, Row>> longbowRichFunctions = new ArrayList<>();
        longbowValidator.validateLongbow(longbowType);
        switch (longbowType) {
            case LongbowWrite:
                longbowWriter = longbowWriterPlus();
                longbowRichFunctions.add(longbowWriter);
                longbowWriter.notifySubscriber(metricsTelemetryExporter);
                return new LongbowProcessor(asyncProcessor, configuration, longbowRichFunctions, new LongbowWriteColumnModifier());
            case LongbowRead:
                longbowReader = longbowReaderPlus();
                longbowRichFunctions.add(longbowReader);
                longbowReader.notifySubscriber(metricsTelemetryExporter);
                return new LongbowProcessor(asyncProcessor, configuration, longbowRichFunctions, new LongbowReadColumnModifier());
            default:
                longbowWriter = longbowWriter();
                longbowReader = longbowReader();
                longbowRichFunctions.add(longbowWriter);
                longbowRichFunctions.add(longbowReader);
                longbowWriter.notifySubscriber(metricsTelemetryExporter);
                longbowReader.notifySubscriber(metricsTelemetryExporter);
                return new LongbowProcessor(asyncProcessor, configuration, longbowRichFunctions, new NoOpColumnModifier());
        }
    }

    private LongbowReader longbowReaderPlus() {
        LongbowRange longbowRange = LongbowRangeFactory.getLongbowRange(longbowSchema);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema, getTableId(configuration));
        ReaderOutputProtoData readerOutputRow = new ReaderOutputProtoData(longbowSchema);
        LongbowProtoData longbowTableData = new LongbowProtoData();
        return new LongbowReader(configuration, longbowSchema, longbowRange, longbowTableData, scanRequestFactory, readerOutputRow);
    }

    private LongbowReader longbowReader() {
        LongbowRange longbowRange = LongbowRangeFactory.getLongbowRange(longbowSchema);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema, getTableId(configuration));
        ReaderOutputLongbowData readerOutputRow = new ReaderOutputLongbowData(longbowSchema);
        LongbowTableData longbowTableData = new LongbowTableData(longbowSchema);
        return new LongbowReader(configuration, longbowSchema, longbowRange, longbowTableData, scanRequestFactory, readerOutputRow);
    }

    private LongbowWriter longbowWriterPlus() {
        ProtoSerializer protoSerializer = new ProtoSerializer(null, getMessageProtoClassName(configuration), columnNames, stencilClientOrchestrator);
        String tableId = getTableId(configuration);
        PutRequestFactory putRequestFactory = new PutRequestFactory(longbowSchema, protoSerializer, tableId);
        OutputSynchronizer outputSynchronizer = new OutputSynchronizer(longbowSchema, tableId, getMessageProtoClassName(configuration));
        return new LongbowWriter(configuration, longbowSchema, putRequestFactory, tableId, outputSynchronizer);
    }

    private LongbowWriter longbowWriter() {
        String tableId = getTableId(configuration);
        PutRequestFactory putRequestFactory = new PutRequestFactory(longbowSchema, null, tableId);
        OutputIdentity outputIdentity = new OutputIdentity();
        return new LongbowWriter(configuration, longbowSchema, putRequestFactory, tableId, outputIdentity);
    }

    private String getTableId(Configuration configuration) {
        return configuration.getString(LONGBOW_GCP_TABLE_ID_KEY, configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT));
    }

    private String getMessageProtoClassName(Configuration configuration) {
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Map[] streamsConfig = gson.fromJson(jsonArrayString, Map[].class);
        return (String) streamsConfig[0].get(STREAM_PROTO_CLASS_NAME);
    }
}
