package org.raystack.dagger.core.processors.longbow;

import org.raystack.dagger.common.serde.proto.serialization.ProtoSerializer;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import com.google.gson.Gson;
import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.core.processors.longbow.columnmodifier.LongbowReadColumnModifier;
import org.raystack.dagger.core.processors.longbow.columnmodifier.LongbowWriteColumnModifier;
import org.raystack.dagger.core.processors.longbow.columnmodifier.NoOpColumnModifier;
import org.raystack.dagger.core.processors.longbow.data.LongbowProtoData;
import org.raystack.dagger.core.processors.longbow.data.LongbowTableData;
import org.raystack.dagger.core.processors.longbow.outputRow.OutputIdentity;
import org.raystack.dagger.core.processors.longbow.outputRow.OutputSynchronizer;
import org.raystack.dagger.core.processors.longbow.outputRow.ReaderOutputLongbowData;
import org.raystack.dagger.core.processors.longbow.outputRow.ReaderOutputProtoData;
import org.raystack.dagger.core.processors.longbow.processor.LongbowReader;
import org.raystack.dagger.core.processors.longbow.processor.LongbowWriter;
import org.raystack.dagger.core.processors.longbow.range.LongbowRange;
import org.raystack.dagger.core.processors.longbow.range.LongbowRangeFactory;
import org.raystack.dagger.core.processors.longbow.request.PutRequestFactory;
import org.raystack.dagger.core.processors.longbow.request.ScanRequestFactory;
import org.raystack.dagger.core.processors.longbow.validator.LongbowType;
import org.raystack.dagger.core.processors.longbow.validator.LongbowValidator;
import org.raystack.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import org.raystack.dagger.core.processors.types.PostProcessor;

import java.util.ArrayList;
import java.util.Map;

import static org.raystack.dagger.common.core.Constants.INPUT_STREAMS;
import static org.raystack.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_PROTO_CLASS;
import static org.raystack.dagger.core.utils.Constants.DAGGER_NAME_DEFAULT;
import static org.raystack.dagger.core.utils.Constants.DAGGER_NAME_KEY;
import static org.raystack.dagger.core.utils.Constants.PROCESSOR_LONGBOW_GCP_TABLE_ID_KEY;

/**
 * The factory class for Longbow.
 */
public class LongbowFactory {
    private LongbowSchema longbowSchema;
    private Configuration configuration;
    private AsyncProcessor asyncProcessor;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private MetricsTelemetryExporter metricsTelemetryExporter;
    private String[] columnNames;
    private static final Gson GSON = new Gson();

    /**
     * Instantiates a new Longbow factory.
     *
     * @param longbowSchema             the longbow schema
     * @param configuration                    the configuration
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param metricsTelemetryExporter  the metrics telemetry exporter
     */
    public LongbowFactory(LongbowSchema longbowSchema, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, MetricsTelemetryExporter metricsTelemetryExporter) {
        this.longbowSchema = longbowSchema;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.columnNames = longbowSchema.getColumnNames().toArray(new String[0]);
        this.asyncProcessor = new AsyncProcessor();
    }

    /**
     * Instantiates a new Longbow factory.
     *
     * @param longbowSchema             the longbow schema
     * @param configuration             the configuration
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param metricsTelemetryExporter  the metrics telemetry exporter
     * @param asyncProcessor            the async processor
     */
    public LongbowFactory(LongbowSchema longbowSchema, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, MetricsTelemetryExporter metricsTelemetryExporter, AsyncProcessor asyncProcessor) {
        this(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter);
        this.longbowSchema = longbowSchema;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.columnNames = longbowSchema.getColumnNames().toArray(new String[0]);
        this.asyncProcessor = asyncProcessor;
    }

    /**
     * Gets longbow processor.
     *
     * @return the longbow processor
     */
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

    private String getTableId(Configuration config) {
        return config
                .getString(PROCESSOR_LONGBOW_GCP_TABLE_ID_KEY, config.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT));
    }

    private String getMessageProtoClassName(Configuration config) {
        String jsonArrayString = config.getString(INPUT_STREAMS, "");
        Map[] streamsConfig = GSON.fromJson(jsonArrayString, Map[].class);
        return (String) streamsConfig[0].get(STREAM_INPUT_SCHEMA_PROTO_CLASS);
    }
}
