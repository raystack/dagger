package io.odpf.dagger.common.serde.proto.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.serde.DaggerSerializationException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ProtoSerializer implements KafkaRecordSerializationSchema<Row> {
    private final String outputTopic;
    private final ProtoSerializerHelper protoSerializerHelper;
    private static final Logger LOGGER = LoggerFactory.getLogger("KafkaSink");

    /**
     * Instantiates a new Proto serializer with specified output topic name.
     *
     * @param keyProtoClassName         the key proto class name
     * @param messageProtoClassName     the message proto class name
     * @param columnNames               the column names
     * @param stencilClientOrchestrator the stencil client orchestrator
     */
    public ProtoSerializer(String keyProtoClassName, String messageProtoClassName, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator) {
        this(keyProtoClassName, messageProtoClassName, columnNames, stencilClientOrchestrator, "");
    }

    /**
     * Instantiates a new Proto serializer with specified output topic name.
     *
     * @param keyProtoClassName         the key proto class name
     * @param messageProtoClassName     the message proto class name
     * @param columnNames               the column names
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param outputTopic               the output topic
     */
    public ProtoSerializer(String keyProtoClassName, String messageProtoClassName, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator, String outputTopic) {
        if (Objects.isNull(messageProtoClassName)) {
            throw new DaggerSerializationException("messageProtoClassName is required");
        }
        this.outputTopic = outputTopic;
        this.protoSerializerHelper = new ProtoSerializerHelper(keyProtoClassName, columnNames, stencilClientOrchestrator, messageProtoClassName);
    }

    @Override
    public void open(InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Row row, KafkaSinkContext context, Long timestamp) {
        if (Objects.isNull(outputTopic) || outputTopic.equals("")) {
            throw new DaggerSerializationException("outputTopic is required");
        }
        LOGGER.info("row to kafka: " + row);
        byte[] key = protoSerializerHelper.serializeKey(row);
        byte[] message = protoSerializerHelper.serializeValue(row);
        return new ProducerRecord<>(outputTopic, key, message);
    }

    public byte[] serializeKey(Row row) {
        return protoSerializerHelper.serializeKey(row);
    }

    public byte[] serializeValue(Row row) {
        return protoSerializerHelper.serializeValue(row);
    }
}
