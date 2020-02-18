package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.data.LongbowDataFactory;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowReader;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowWriter;
import com.gojek.daggers.postProcessors.longbow.request.PutRequestFactory;
import com.gojek.daggers.postProcessors.longbow.request.ScanRequestFactory;
import com.gojek.daggers.postProcessors.longbow.row.LongbowRow;
import com.gojek.daggers.postProcessors.longbow.row.LongbowRowFactory;
import com.gojek.daggers.postProcessors.longbow.validator.LongbowType;
import com.gojek.daggers.postProcessors.longbow.validator.LongbowValidator;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.sink.ProtoSerializer;
import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

import static com.gojek.daggers.utils.Constants.INPUT_STREAMS;
import static com.gojek.daggers.utils.Constants.STREAM_PROTO_CLASS_NAME;

public class LongbowProcessorFactory {
    private LongbowSchema longbowSchema;
    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private MetricsTelemetryExporter metricsTelemetryExporter;
    private String[] columnNames;


    public LongbowProcessorFactory(LongbowSchema longbowSchema, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, MetricsTelemetryExporter metricsTelemetryExporter, String[] columnNames) {
        this.longbowSchema = longbowSchema;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.metricsTelemetryExporter = metricsTelemetryExporter;
        this.columnNames = columnNames;
    }

    public PostProcessor getLongbowProcessor() {
        LongbowReader longbowReader;
        LongbowWriter longbowWriter;
        LongbowValidator longbowValidator = new LongbowValidator(columnNames);

        AsyncProcessor asyncProcessor = new AsyncProcessor();
        //TODO : Think more on LongbowReader validation and how to incorporate LonbowRow in validation
        if (longbowSchema.contains(LongbowType.LongbowWrite.getTypeValue())) {
            longbowValidator.validateLongbow(LongbowType.LongbowWrite);

            longbowWriter = getLongbowWriter(configuration, longbowSchema, columnNames, stencilClientOrchestrator);
            longbowWriter.notifySubscriber(metricsTelemetryExporter);
            return new LongbowWriteProcessor(longbowWriter, asyncProcessor, configuration, getMessageProtoClassName(configuration));
        } else if (longbowSchema.contains(LongbowType.LongbowWrite.getTypeValue())) {
            longbowValidator.validateLongbow(LongbowType.LongbowRead);

            longbowReader = getLongbowReader(configuration, longbowSchema);
            longbowReader.notifySubscriber(metricsTelemetryExporter);
            return new LongbowReadProcessor(longbowReader, asyncProcessor, configuration);
        } else {
            longbowValidator.validateLongbow(LongbowType.LongbowProcess);

            longbowReader = getLongbowReader(configuration, longbowSchema);
            longbowWriter = getLongbowWriter(configuration, longbowSchema, columnNames, stencilClientOrchestrator);
            longbowWriter.notifySubscriber(metricsTelemetryExporter);
            longbowReader.notifySubscriber(metricsTelemetryExporter);
            return new LongbowProcessor(longbowWriter, longbowReader, asyncProcessor, configuration);
        }
    }

    private LongbowReader getLongbowReader(Configuration configuration, LongbowSchema longbowSchema) {
        LongbowDataFactory longbowDataFactory = new LongbowDataFactory(longbowSchema);
        LongbowRow longbowRow = LongbowRowFactory.getLongbowRow(longbowSchema);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema);
        return new LongbowReader(configuration, longbowSchema, longbowRow, longbowDataFactory.getLongbowData(), scanRequestFactory);
    }

    private LongbowWriter getLongbowWriter(Configuration configuration, LongbowSchema longbowSchema, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator) {
        ProtoSerializer protoSerializer = new ProtoSerializer(null, getMessageProtoClassName(configuration), columnNames, stencilClientOrchestrator);
        return new LongbowWriter(configuration, longbowSchema, new PutRequestFactory(longbowSchema, protoSerializer));
    }

    private String getMessageProtoClassName(Configuration configuration) {
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Gson gson = new Gson();
        Map[] streamsConfig = gson.fromJson(jsonArrayString, Map[].class);
        return (String) streamsConfig[0].get(STREAM_PROTO_CLASS_NAME);
    }
}
