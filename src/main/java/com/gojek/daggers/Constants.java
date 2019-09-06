package com.gojek.daggers;

public class Constants {
    public final static String EXTERNAL_SOURCE_ENABLED_KEY = "EXTERNAL_SOURCE_ENABLED";
    public final static boolean EXTERNAL_SOURCE_ENABLED_KEY_DEFAULT = false;
    public final static String EXTERNAL_SOURCE_HTTP_ENDPOINT = "endpoint";
    public final static String EXTERNAL_SOURCE_HTTP_VERB = "verb";
    public final static String EXTERNAL_SOURCE_HTTP_BODY_FIELD = "body_field";
    public final static String EXTERNAL_SOURCE_OUTPUT_MAPPING = "output_mapping";
    public final static String EXTERNAL_SOURCE_OUTPUT_MAPPING_PATH = "path";
    public final static String ASYNC_IO_HTTP_CONNECT_TIMEOUT_KEY = "connect_timeout";
    public final static String ASYNC_IO_HTTP_CONNECT_TIMEOUT_DEFAULT = "1000";
    public final static String EXTERNAL_SOURCE_KEY = "EXTERNAL_SOURCE";
    public final static String ASYNC_IO_ENABLED_KEY = "ASYNC_IO_ENABLED";
    public final static boolean ASYNC_IO_ENABLED_DEFAULT = false;
    public final static String ASYNC_IO_ES_HOST_KEY = "host";
    public final static String ASYNC_IO_ES_HOST_DEFAULT = "";
    public final static String ASYNC_IO_ES_CONNECT_TIMEOUT_KEY = "connect_timeout";
    public final static String ASYNC_IO_ES_MAX_RETRY_TIMEOUT_KEY = "retry_timeout";
    public final static String ASYNC_IO_ES_SOCKET_TIMEOUT_KEY = "socket_timeout";
    public final static String ASYNC_IO_ES_INPUT_INDEX_KEY = "input_index";
    public final static String ASYNC_IO_ES_PATH_KEY = "path";
    public final static String ASYNC_IO_CAPACITY_KEY = "capacity";
    public final static String ASYNC_IO_CAPACITY_DEFAULT = "30";
    public final static String ASYNC_IO_KEY = "ASYNC_IO";
    public final static String OUTPUT_PROTO_CLASS_PREFIX_KEY = "OUTPUT_PROTO_CLASS_PREFIX";
    public final static String OUTPUT_KAFKA_TOPIC = "OUTPUT_KAFKA_TOPIC";
    public final static String OUTPUT_KAFKA_BROKER = "OUTPUT_KAFKA_BROKER";
    public final static String FIELD_NAME_KEY = "field_name";
    public final static String LONGBOW_KEY = "longbow_key";
    public final static String LONGBOW_DURATION = "longbow_duration";
    public final static String LONGBOW_LATEST = "longbow_latest";
    public final static String LONGBOW_EARLIEST = "longbow_earliest";
    public final static String LONGBOW_DOCUMENT_DURATION = "LONGBOW_DOCUMENT_DURATION";
    public final static String LONGBOW_DOCUMENT_DURATION_DEFAULT = "90d";
    public final static String LONGBOW_DELIMITER = "#";
    public final static String LONGBOW_DATA = "longbow_data";
    public final static String LONGBOW_GCP_PROJECT_ID_KEY = "LONGBOW_GCP_PROJECT_ID";
    public final static String LONGBOW_GCP_PROJECT_ID_DEFAULT = "godata-production";
    public final static String LONGBOW_GCP_INSTANCE_ID_KEY = "LONGBOW_GCP_INSTANCE_ID";
    public final static String LONGBOW_GCP_INSTANCE_ID_DEFAULT = "godata-id-daggers";
    public final static String LONGBOW_COLUMN_FAMILY_DEFAULT = "ts";
    public final static Long LONGBOW_ASYNC_TIMEOUT_DEFAULT = 15000L;
    public final static String LONGBOW_ASYNC_TIMEOUT_KEY = "LONGBOW_ASYNC_TIMEOUT";
    public final static Integer LONGBOW_THREAD_CAPACITY_DEFAULT = 30;
    public final static String LONGBOW_THREAD_CAPACITY_KEY = "LONGBOW_THREAD_CAPACITY";
    public final static String DAGGER_NAME_KEY = "FLINK_JOB_ID";
    public final static String DAGGER_NAME_DEFAULT = "SQL Flink Job";
    public final static String EVENT_TIMESTAMP = "event_timestamp";
    public final static String ROWTIME = "rowtime";
    public final static String HOUR_UNIT = "h";
    public final static String DAY_UNIT = "d";
    public final static String SQL_QUERY = "SQL_QUERY";
    public final static String SQL_QUERY_DEFAULT = "";

    public final static String PORTAL_VERSION = "PORTAL_VERSION";
    public final static String OUTPUT_PROTO_KEY = "OUTPUT_PROTO_KEY";
    public final static String OUTPUT_PROTO_MESSAGE = "OUTPUT_PROTO_MESSAGE";
}
