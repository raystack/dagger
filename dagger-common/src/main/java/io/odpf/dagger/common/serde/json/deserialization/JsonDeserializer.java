package io.odpf.dagger.common.serde.json.deserialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;

import static io.odpf.dagger.common.core.Constants.ROWTIME;

public class JsonDeserializer implements KafkaDeserializationSchema<Row> {
    private final JsonRowDeserializationSchema jsonRowDeserializationSchema;
    private final int rowtimeIdx;
    private final TypeInformation<Row> typeInformation;

    public JsonDeserializer(String jsonSchema, String rowtimeFieldName) {
        this.typeInformation = new JsonType(jsonSchema, ROWTIME).getRowType();
        this.jsonRowDeserializationSchema = new JsonRowDeserializationSchema.Builder(typeInformation).build();
        RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInformation;
        this.rowtimeIdx = rowTypeInfo.getFieldIndex(rowtimeFieldName);
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public Row deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        try {
            Row inputRow = jsonRowDeserializationSchema.deserialize(consumerRecord.value());
            return addTimestampFieldToRow(inputRow);
        } catch (RuntimeException | IOException e) {
            throw new DaggerDeserializationException(e);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return jsonRowDeserializationSchema.getProducedType();
    }

    private Row addTimestampFieldToRow(Row row) {
        Row finalRecord = new Row(row.getArity());

        for (int i = 0; i < row.getArity() - 2; i++) {
            finalRecord.setField(i, row.getField(i));
        }

        Object rowtimeField = row.getField(rowtimeIdx);
        if (rowtimeField instanceof BigDecimal) {
            BigDecimal bigDecimalField = (BigDecimal) row.getField(rowtimeIdx);
            finalRecord.setField(finalRecord.getArity() - 1, Timestamp.from(Instant.ofEpochSecond(bigDecimalField.longValue())));
        } else if (rowtimeField instanceof Timestamp) {
            finalRecord.setField(finalRecord.getArity() - 1, rowtimeField);
        } else {
            throw new DaggerDeserializationException("Invalid Rowtime datatype for rowtimeField");
        }
        finalRecord.setField(finalRecord.getArity() - 2, true);

        return finalRecord;
    }
}
