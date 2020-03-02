package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.data.LongbowDataFactory;
import com.gojek.daggers.postProcessors.longbow.outputRow.OutputLongbowData;
import com.gojek.daggers.postProcessors.longbow.outputRow.OutputProtoData;
import com.gojek.daggers.postProcessors.longbow.outputRow.OutputRow;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowReader;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowWriter;
import com.gojek.daggers.postProcessors.longbow.request.PutRequestFactory;
import com.gojek.daggers.postProcessors.longbow.request.ScanRequestFactory;
import com.gojek.daggers.postProcessors.longbow.row.LongbowRange;
import com.gojek.daggers.postProcessors.longbow.row.LongbowRangeFactory;
import com.gojek.daggers.postProcessors.longbow.validator.LongbowType;
import com.gojek.daggers.postProcessors.longbow.validator.LongbowValidator;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.sink.ProtoSerializer;
import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowProcessorFactory {
    private LongbowSchema longbowSchema;
    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private MetricsTelemetryExporter metricsTelemetryExporter;
    private String[] columnNames;


    public LongbowProcessorFactory(LongbowSchema longbowSchema, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, MetricsTelemetryExporter metricsTelemetryExporter) {
        this.longbowSchema = longbowSchema;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.columnNames = longbowSchema.getColumnNames().toArray(new String[0]);
    }

    public PostProcessor getLongbowProcessor() {
        // Remove further encapsulation, pull all the component here to make flow clear
        LongbowReader longbowReader;
        LongbowWriter longbowWriter;
        LongbowValidator longbowValidator = new LongbowValidator(columnNames);
        LongbowType longbowType = longbowSchema.getType();

        AsyncProcessor asyncProcessor = new AsyncProcessor();
        longbowValidator.validateLongbow(longbowType);
        switch (longbowType) {
            // Make single longbowprocessor, pass richmap and columnnames modifier
            case LongbowWrite:
                longbowWriter = getLongbowWriter(configuration, longbowSchema, columnNames, stencilClientOrchestrator, longbowSchema.isLongbowPlus());
                longbowWriter.notifySubscriber(metricsTelemetryExporter);
                return new LongbowWriteProcessor(longbowWriter, asyncProcessor, configuration, getMessageProtoClassName(configuration), longbowSchema, getTableId(configuration));
            case LongbowRead:
                longbowReader = getLongbowReader(configuration, longbowSchema);
                longbowReader.notifySubscriber(metricsTelemetryExporter);
                return new LongbowReadProcessor(longbowReader, asyncProcessor, configuration);
            default:
                longbowReader = getLongbowReader(configuration, longbowSchema);
                longbowWriter = getLongbowWriter(configuration, longbowSchema, columnNames, stencilClientOrchestrator, longbowSchema.isLongbowPlus());
                longbowWriter.notifySubscriber(metricsTelemetryExporter);
                longbowReader.notifySubscriber(metricsTelemetryExporter);
                return new LongbowProcessor(longbowWriter, longbowReader, asyncProcessor, configuration);
        }
    }

    private LongbowReader getLongbowReader(Configuration configuration, LongbowSchema longbowSchema) {
        LongbowDataFactory longbowDataFactory = new LongbowDataFactory(longbowSchema);
        LongbowRange longbowRange = LongbowRangeFactory.getLongbowRange(longbowSchema);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema, getTableId(configuration));
        OutputRow outputRow = new OutputLongbowData(longbowSchema);
        if (longbowSchema.isLongbowPlus()) {
            outputRow = new OutputProtoData(longbowSchema);
        }
        return new LongbowReader(configuration, longbowSchema, longbowRange, longbowDataFactory.getLongbowData(), scanRequestFactory, outputRow);
    }

    private LongbowWriter getLongbowWriter(Configuration configuration, LongbowSchema longbowSchema, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator, boolean isLongbowPlus) {
        ProtoSerializer protoSerializer = null;
        if (isLongbowPlus) {
            protoSerializer = new ProtoSerializer(null, getMessageProtoClassName(configuration), columnNames, stencilClientOrchestrator);
        }
        return new LongbowWriter(configuration, longbowSchema, new PutRequestFactory(longbowSchema, protoSerializer, getTableId(configuration)), getTableId(configuration));
    }

    private String getTableId(Configuration configuration) {
        return configuration.getString(LONGBOW_GCP_TABLE_ID_KEY, configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT));
    }

    private String getMessageProtoClassName(Configuration configuration) {
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Gson gson = new Gson();
        Map[] streamsConfig = gson.fromJson(jsonArrayString, Map[].class);
        return (String) streamsConfig[0].get(STREAM_PROTO_CLASS_NAME);
    }
}
