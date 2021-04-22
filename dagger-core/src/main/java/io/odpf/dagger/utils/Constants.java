package io.odpf.dagger.utils;

public class Constants {
    public final static String INTERNAL_VALIDATION_FILED = "__internal_validation_field__";
    public final static String PRE_PROCESSOR_ENABLED_KEY = "PRE_PROCESSOR_ENABLED";
    public final static boolean PRE_PROCESSOR_ENABLED_DEFAULT = false;
    public final static String PRE_PROCESSOR_CONFIG_KEY = "PRE_PROCESSOR_CONFIG";
    public final static String TELEMETRY_ENABLED_KEY = "TELEMETRY_ENABLED";
    public final static boolean TELEMETRY_ENABLED_VALUE_DEFAULT = true;
    public final static String POST_PROCESSOR_ENABLED_KEY = "POST_PROCESSOR_ENABLED";
    public final static boolean POST_PROCESSOR_ENABLED_KEY_DEFAULT = false;
    public final static String POST_PROCESSOR_CONFIG_KEY = "POST_PROCESSOR_CONFIG";
    public final static String ASYNC_IO_ENABLED_KEY = "ASYNC_IO_ENABLED";
    public final static boolean ASYNC_IO_ENABLED_DEFAULT = false;
    public final static String ASYNC_IO_CAPACITY_KEY = "capacity";
    public final static String ASYNC_IO_CAPACITY_DEFAULT = "30";
    public final static String OUTPUT_KAFKA_TOPIC = "OUTPUT_KAFKA_TOPIC";
    public final static String OUTPUT_KAFKA_BROKER = "OUTPUT_KAFKA_BROKER";
    public final static String LONGBOW_DURATION = "longbow_duration";
    public final static String LONGBOW_LATEST = "longbow_latest";
    public final static String LONGBOW_EARLIEST = "longbow_earliest";
    public final static String LONGBOW_DOCUMENT_DURATION = "LONGBOW_DOCUMENT_DURATION";
    public final static String LONGBOW_DOCUMENT_DURATION_DEFAULT = "90d";
    public final static String LONGBOW_DELIMITER = "#";
    public final static String LONGBOW_DATA = "longbow_data";
    public final static String LONGBOW_PROTO_DATA = "proto_data";
    public final static String LONGBOW_GCP_PROJECT_ID_KEY = "LONGBOW_GCP_PROJECT_ID";
    public final static String LONGBOW_GCP_PROJECT_ID_DEFAULT = "godata-production";
    public final static String LONGBOW_GCP_INSTANCE_ID_KEY = "LONGBOW_GCP_INSTANCE_ID";
    public final static String LONGBOW_GCP_INSTANCE_ID_DEFAULT = "godata-id-daggers";
    public final static String LONGBOW_GCP_TABLE_ID_KEY = "LONGBOW_GCP_TABLE_ID_KEY";
    public final static String LONGBOW_COLUMN_FAMILY_DEFAULT = "ts";
    public final static String LONGBOW_QUALIFIER_DEFAULT = "proto";
    public final static Long LONGBOW_ASYNC_TIMEOUT_DEFAULT = 15000L;
    public final static String LONGBOW_ASYNC_TIMEOUT_KEY = "LONGBOW_ASYNC_TIMEOUT";
    public final static Integer LONGBOW_THREAD_CAPACITY_DEFAULT = 30;
    public final static String LONGBOW_THREAD_CAPACITY_KEY = "LONGBOW_THREAD_CAPACITY";
    public final static String DAGGER_NAME_KEY = "FLINK_JOB_ID";
    public final static String DAGGER_NAME_DEFAULT = "SQL Flink Job";
    public final static String EVENT_TIMESTAMP = "event_timestamp";
    public final static String ROWTIME = "rowtime";
    public final static String MINUTE_UNIT = "m";
    public final static String HOUR_UNIT = "h";
    public final static String DAY_UNIT = "d";
    public final static String SQL_QUERY = "SQL_QUERY";
    public final static String SQL_QUERY_DEFAULT = "";
    public static final int MAX_PARALLELISM_DEFAULT = 50;
    public static final String MAX_PARALLELISM_KEY = "MAX_PARALLELISM";

    public final static String SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY = "bigtable_table_id";
    public final static String SYNCHRONIZER_INPUT_CLASSNAME_KEY = "input_class_name";
    public final static String SYNCHRONIZER_LONGBOWREAD_KEY = "longbow_read_key";

    public final static String OUTPUT_PROTO_KEY = "OUTPUT_PROTO_KEY";
    public final static String OUTPUT_PROTO_MESSAGE = "OUTPUT_PROTO_MESSAGE";
    public final static String OUTPUT_STREAM = "OUTPUT_STREAM";
    public final static String ES_TYPE = "ES";
    public final static String HTTP_TYPE = "HTTP";
    public final static String PG_TYPE = "PG";
    public final static String GRPC_TYPE = "GRPC";
    public final static String SQL_PATH_SELECT_ALL_CONFIG_VALUE = "*";

    public final static String LONGBOW_WRITER_PROCESSOR = "longbow_writer_processor";
    public final static String LONGBOW_READER_PROCESSOR = "longbow_reader_processor";
    public final static String TRANSFORM_PROCESSOR = "transform_processor";
    public final static String SQL_TRANSFORMER_CLASS = "com.gojek.dagger.transformer.SQLTransformer";

    public static final String STREAM_PROTO_CLASS_NAME = "PROTO_CLASS_NAME";
    public static final String STREAM_TABLE_NAME = "TABLE_NAME";
    public static final String STREAM_TOPIC_NAMES = "TOPIC_NAMES";
    public static final String INPUT_STREAMS = "STREAMS";
    public static final String INPUT_STREAM_NAME = "STREAM_NAME";

    public static final String SHUTDOWN_PERIOD_KEY = "SHUTDOWN_PERIOD";
    public static final long SHUTDOWN_PERIOD_DEFAULT = 10000;
    public static final String FATAL_EXCEPTION_METRIC_GROUP_KEY = "fatal.exception";
    public static final String NONFATAL_EXCEPTION_METRIC_GROUP_KEY = "non.fatal.exception";

    public static final String FUNCTION_FACTORY_CLASSES_KEY = "FUNCTION_FACTORY_CLASSES";
    public static final String FUNCTION_FACTORY_CLASSES_DEFAULT = "";

    public static final String INFLUX_LATE_RECORDS_DROPPED_KEY = "influx.late.records.dropped";

    public static final String STENCIL_ENABLE_KEY = "ENABLE_STENCIL_URL";
    public static final boolean STENCIL_ENABLE_DEFAULT = false;
    public static final String STENCIL_URL_KEY = "STENCIL_URL";
    public static final String STENCIL_URL_DEFAULT = "";

    public static final String STENCIL_CONFIG_TTL_IN_MINUTES_KEY = "TIL_IN_MINUTES";
    public static final String STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT = "0";
    public static final String STENCIL_CONFIG_TIMEOUT_MS_KEY = "STENCIL_TIMEOUT_MS";
    public static final String STENCIL_CONFIG_TIMEOUT_MS_DEFAULT = "60000";
    public static final String STENCIL_CONFIG_REFRESH_CACHE_KEY = "REFRESH_CACHE";
    public static final String STENCIL_CONFIG_REFRESH_CACHE_DEFAULT = "false";

    public static final String PRODUCE_LARGE_MESSAGE_KEY = "PRODUCE_LARGE_MESSAGE";
    public static final boolean PRODUCE_LARGE_MESSAGE_DEFAULT = false;
    public static final String CONSUME_LARGE_MESSAGE_KEY = "CONSUME_LARGE_MESSAGE";
    public static final boolean CONSUME_LARGE_MESSAGE_DEFAULT = false;


    public final static String GCS_PROJECT_ID = "GCS_PROJECT_ID";
    public final static String GCS_PROJECT_DEFAULT = "godata-production";

    public final static String GCS_BUCKET_ID = "GCS_BUCKET_ID";
    public final static String GCS_BUCKET_DEFAULT = "p-godata-daggers-darts-storage";
}