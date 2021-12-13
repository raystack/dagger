package io.odpf.dagger.core.processors.external.es;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.odpf.dagger.common.serde.proto.protohandler.ProtoHandler;
import io.odpf.dagger.common.serde.proto.protohandler.ProtoHandlerFactory;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.exception.HttpFailureException;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.PostResponseTelemetry;
import io.odpf.dagger.core.processors.common.RowManager;
import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.common.serde.proto.protohandler.RowFactory.createRow;
import static java.util.Collections.singleton;
import static org.apache.http.HttpStatus.SC_OK;

/**
 * The ElasticSearch response handler.
 */
public class EsResponseHandler implements ResponseListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsResponseHandler.class.getName());
    private EsSourceConfig esSourceConfig;
    private RowManager rowManager;
    private Descriptor outputDescriptor;
    private ResultFuture<Row> resultFuture;
    private Instant startTime;
    private MeterStatsManager meterStatsManager;
    private ColumnNameManager columnNameManager;
    private ErrorReporter errorReporter;
    private PostResponseTelemetry postResponseTelemetry;

    /**
     * Instantiates a new ElasticSearch response handler.
     *
     * @param esSourceConfig        the es source config
     * @param meterStatsManager     the meter stats manager
     * @param rowManager            the row manager
     * @param columnNameManager     the column name manager
     * @param outputDescriptor      the output descriptor
     * @param resultFuture          the result future
     * @param errorStatsReporter    the error stats reporter
     * @param postResponseTelemetry the post response telemetry
     */
    public EsResponseHandler(EsSourceConfig esSourceConfig, MeterStatsManager meterStatsManager, RowManager rowManager, ColumnNameManager columnNameManager, Descriptor outputDescriptor, ResultFuture<Row> resultFuture, ErrorReporter errorStatsReporter, PostResponseTelemetry postResponseTelemetry) {
        this.esSourceConfig = esSourceConfig;
        this.rowManager = rowManager;
        this.outputDescriptor = outputDescriptor;
        this.resultFuture = resultFuture;
        this.meterStatsManager = meterStatsManager;
        this.columnNameManager = columnNameManager;
        this.errorReporter = errorStatsReporter;
        this.postResponseTelemetry = postResponseTelemetry;
    }

    /**
     * Start timer.
     */
    public void startTimer() {
        startTime = Instant.now();
    }

    @Override
    public void onSuccess(Response response) {
        try {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                return;
            }
            String responseBody = EntityUtils.toString(response.getEntity());
            List<String> esOutputColumnNames = esSourceConfig.getOutputColumns();
            esOutputColumnNames.forEach(outputColumnName -> {
                String outputColumnPath = esSourceConfig.getPath(outputColumnName);
                Object outputValue;
                try {
                    outputValue = JsonPath.parse(responseBody).read(outputColumnPath, new Object().getClass());
                } catch (PathNotFoundException exception) {
                    postResponseTelemetry.failureReadingPath(meterStatsManager);
                    LOGGER.error(exception.getMessage());
                    reportAndThrowError(exception);
                    return;
                }
                int outputColumnIndex = columnNameManager.getOutputIndex(outputColumnName);
                setField(esSourceConfig, outputColumnIndex, outputValue, outputColumnName);
            });
        } catch (ParseException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.ERROR_PARSING_RESPONSE);
            System.err.printf("ESResponseHandler : error parsing response, error msg : %s, response : %s\n", e.getMessage(), response.toString());
            errorReporter.reportNonFatalException(e);
            e.printStackTrace();
        } catch (IOException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.ERROR_READING_RESPONSE);
            System.err.printf("ESResponseHandler : error reading response, error msg : %s, response : %s\n", e.getMessage(), response.toString());
            errorReporter.reportNonFatalException(e);
            e.printStackTrace();
        } catch (Exception e) {
            meterStatsManager.markEvent(ExternalSourceAspects.OTHER_ERRORS_PROCESSING_RESPONSE);
            System.err.printf("ESResponseHandler : other errors processing response, error msg : %s, response : %s\n", e.getMessage(), response.toString());
            errorReporter.reportNonFatalException(e);
            e.printStackTrace();
        } finally {
            postResponseTelemetry.sendSuccessTelemetry(meterStatsManager, startTime);
            resultFuture.complete(singleton(rowManager.getAll()));
        }
    }

    @Override
    public void onFailure(Exception e) {
        postResponseTelemetry.sendFailureTelemetry(meterStatsManager, startTime);
        Exception httpFailureException = new HttpFailureException("EsResponseHandler : Failed with error. " + e.getMessage());
        if (esSourceConfig.isFailOnErrors()) {
            reportAndThrowError(httpFailureException);
        } else {
            errorReporter.reportNonFatalException(e);
        }
        if (e instanceof ResponseException) {
            postResponseTelemetry.validateResponseCode(meterStatsManager, ((ResponseException) e).getResponse().getStatusLine().getStatusCode());
        } else {
            meterStatsManager.markEvent(ExternalSourceAspects.OTHER_ERRORS);
            System.err.printf("ESResponseHandler some other errors :  %s \n", e.getMessage());
        }
        resultFuture.complete(singleton(rowManager.getAll()));
    }


    private void setField(EsSourceConfig esConfig, int index, Object value, String name) {
        if (!esConfig.isRetainResponseType() || esConfig.hasType()) {
            Descriptors.FieldDescriptor fieldDescriptor = outputDescriptor.findFieldByName(name);
            if (fieldDescriptor == null) {
                Exception illegalArgumentException = new IllegalArgumentException("Field Descriptor not found for field: " + name);
                reportAndThrowError(illegalArgumentException);
                meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
                return;
            }
            if (value instanceof Map) {
                rowManager.setInOutput(index, createRow((Map<String, Object>) value, fieldDescriptor.getMessageType()));
            } else {
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
                rowManager.setInOutput(index, protoHandler.transformFromPostProcessor(value));
            }
        } else {
            rowManager.setInOutput(index, value);
        }
    }

    private void reportAndThrowError(Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }
}
