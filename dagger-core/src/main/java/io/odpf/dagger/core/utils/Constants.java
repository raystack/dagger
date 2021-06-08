package io.odpf.dagger.core.utils;

public class Constants {
    public static final String INTERNAL_VALIDATION_FILED = "__internal_validation_field__";
    public static final String PRE_PROCESSOR_ENABLED_KEY = "PRE_PROCESSOR_ENABLED";
    public static final boolean PRE_PROCESSOR_ENABLED_DEFAULT = false;
    public static final String PRE_PROCESSOR_CONFIG_KEY = "PRE_PROCESSOR_CONFIG";
    public static final String TELEMETRY_ENABLED_KEY = "TELEMETRY_ENABLED";
    public static final boolean TELEMETRY_ENABLED_VALUE_DEFAULT = true;
    public static final String POST_PROCESSOR_ENABLED_KEY = "POST_PROCESSOR_ENABLED";
    public static final boolean POST_PROCESSOR_ENABLED_KEY_DEFAULT = false;
    public static final String POST_PROCESSOR_CONFIG_KEY = "POST_PROCESSOR_CONFIG";
    public static final String ASYNC_IO_ENABLED_KEY = "ASYNC_IO_ENABLED";
    public static final boolean ASYNC_IO_ENABLED_DEFAULT = false;
    public static final String ASYNC_IO_CAPACITY_KEY = "capacity";
    public static final String ASYNC_IO_CAPACITY_DEFAULT = "30";
    public static final String OUTPUT_KAFKA_TOPIC = "OUTPUT_KAFKA_TOPIC";
    public static final String OUTPUT_KAFKA_BROKER = "OUTPUT_KAFKA_BROKER";
    public static final String LONGBOW_DURATION = "longbow_duration";
    public static final String LONGBOW_LATEST = "longbow_latest";
    public static final String LONGBOW_EARLIEST = "longbow_earliest";
    public static final String LONGBOW_DOCUMENT_DURATION = "LONGBOW_DOCUMENT_DURATION";
    public static final String LONGBOW_DOCUMENT_DURATION_DEFAULT = "90d";
    public static final String LONGBOW_DELIMITER = "#";
    public static final String LONGBOW_DATA = "longbow_data";
    public static final String LONGBOW_PROTO_DATA = "proto_data";
    public static final String LONGBOW_GCP_PROJECT_ID_KEY = "LONGBOW_GCP_PROJECT_ID";
    public static final String LONGBOW_GCP_PROJECT_ID_DEFAULT = "godata-production";
    public static final String LONGBOW_GCP_INSTANCE_ID_KEY = "LONGBOW_GCP_INSTANCE_ID";
    public static final String LONGBOW_GCP_INSTANCE_ID_DEFAULT = "godata-id-daggers";
    public static final String LONGBOW_GCP_TABLE_ID_KEY = "LONGBOW_GCP_TABLE_ID_KEY";
    public static final String LONGBOW_COLUMN_FAMILY_DEFAULT = "ts";
    public static final String LONGBOW_QUALIFIER_DEFAULT = "proto";
    public static final Long LONGBOW_ASYNC_TIMEOUT_DEFAULT = 15000L;
    public static final String LONGBOW_ASYNC_TIMEOUT_KEY = "LONGBOW_ASYNC_TIMEOUT";
    public static final Integer LONGBOW_THREAD_CAPACITY_DEFAULT = 30;
    public static final String LONGBOW_THREAD_CAPACITY_KEY = "LONGBOW_THREAD_CAPACITY";
    public static final String DAGGER_NAME_KEY = "FLINK_JOB_ID";
    public static final String DAGGER_NAME_DEFAULT = "SQL Flink Job";
    public static final String EVENT_TIMESTAMP = "event_timestamp";
    public static final String ROWTIME = "rowtime";
    public static final String MINUTE_UNIT = "m";
    public static final String HOUR_UNIT = "h";
    public static final String DAY_UNIT = "d";
    public static final String SQL_QUERY = "SQL_QUERY";
    public static final String SQL_QUERY_DEFAULT = "";

    public static final int PARALLELISM_DEFAULT = 1;
    public static final String PARALLELISM_KEY = "PARALLELISM";
    public static final int MAX_PARALLELISM_DEFAULT = 50;
    public static final String MAX_PARALLELISM_KEY = "MAX_PARALLELISM";
    public static final int WATERMARK_INTERVAL_MS_DEFAULT = 10000;
    public static final String WATERMARK_INTERVAL_MS_KEY = "WATERMARK_INTERVAL_MS";
    public static final long CHECKPOINT_INTERVAL_DEFAULT = 30000;
    public static final String CHECKPOINT_INTERVAL_KEY = "CHECKPOINT_INTERVAL";
    public static final long CHECKPOINT_TIMEOUT_DEFAULT = 900000;
    public static final String CHECKPOINT_TIMEOUT_KEY = "CHECKPOINT_TIMEOUT";
    public static final long CHECKPOINT_MIN_PAUSE_DEFAULT = 5000;
    public static final String CHECKPOINT_MIN_PAUSE_KEY = "CHECKPOINT_MIN_PAUSE";
    public static final int MAX_CONCURRECT_CHECKPOINTS_DEFAULT = 1;
    public static final String MAX_CONCURRECT_CHECKPOINTS_KEY = "MAX_CONCURRECT_CHECKPOINTS";
    public static final int MIN_IDLE_STATE_RETENTION_TIME_DEFAULT = 8;
    public static final String MIN_IDLE_STATE_RETENTION_TIME_KEY = "MIN_IDLE_STATE_RETENTION_TIME";
    public static final int MAX_IDLE_STATE_RETENTION_TIME_DEFAULT = 9;
    public static final String MAX_IDLE_STATE_RETENTION_TIME_KEY = "MAX_IDLE_STATE_RETENTION_TIME";
    public static final long WATERMARK_DELAY_MS_DEFAULT = 10000;
    public static final String WATERMARK_DELAY_MS_KEY = "WATERMARK_DELAY_MS";

    public static final String SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY = "bigtable_table_id";
    public static final String SYNCHRONIZER_INPUT_CLASSNAME_KEY = "input_class_name";
    public static final String SYNCHRONIZER_LONGBOWREAD_KEY = "longbow_read_key";

    public static final String OUTPUT_PROTO_KEY = "OUTPUT_PROTO_KEY";
    public static final String OUTPUT_PROTO_MESSAGE = "OUTPUT_PROTO_MESSAGE";
    public static final String OUTPUT_STREAM = "OUTPUT_STREAM";
    public static final String ES_TYPE = "ES";
    public static final String HTTP_TYPE = "HTTP";
    public static final String PG_TYPE = "PG";
    public static final String GRPC_TYPE = "GRPC";
    public static final String SQL_PATH_SELECT_ALL_CONFIG_VALUE = "*";

    public static final String LONGBOW_WRITER_PROCESSOR = "longbow_writer_processor";
    public static final String LONGBOW_READER_PROCESSOR = "longbow_reader_processor";
    public static final String TRANSFORM_PROCESSOR = "transform_processor";
    public static final String SQL_TRANSFORMER_CLASS = "io.odpf.dagger.functions.transformers.SQLTransformer";

    public static final String STREAM_TOPIC_NAMES = "TOPIC_NAMES";
    public static final String INPUT_STREAM_NAME = "STREAM_NAME";

    public static final String SHUTDOWN_PERIOD_KEY = "SHUTDOWN_PERIOD";
    public static final long SHUTDOWN_PERIOD_DEFAULT = 10000;
    public static final String FATAL_EXCEPTION_METRIC_GROUP_KEY = "fatal.exception";
    public static final String NONFATAL_EXCEPTION_METRIC_GROUP_KEY = "non.fatal.exception";

    public static final String FUNCTION_FACTORY_CLASSES_KEY = "FUNCTION_FACTORY_CLASSES";
    public static final String FUNCTION_FACTORY_CLASSES_DEFAULT = "io.odpf.dagger.functions.udfs.factories.FunctionFactory";

    public static final String INFLUX_LATE_RECORDS_DROPPED_KEY = "influx.late.records.dropped";

    public static final String PRODUCE_LARGE_MESSAGE_KEY = "PRODUCE_LARGE_MESSAGE";
    public static final boolean PRODUCE_LARGE_MESSAGE_DEFAULT = false;
    public static final String CONSUME_LARGE_MESSAGE_KEY = "CONSUME_LARGE_MESSAGE";
    public static final boolean CONSUME_LARGE_MESSAGE_DEFAULT = false;

    public static final int CLIENT_ERROR_MIN_STATUS_CODE = 400;
    public static final int CLIENT_ERROR_MAX_STATUS_CODE = 499;
    public static final int SERVER_ERROR_MIN_STATUS_CODE = 500;
    public static final int SERVER_ERROR_MAX_STATUS_CODE = 599;

    public static final long MAX_EVENT_LOOP_EXECUTE_TIME_DEFAULT = 10000;
    public static final int LONGBOW_OUTPUT_ADDITIONAL_ARITY = 3;
}
