package io.odpf.dagger.common.serde.json.deserialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.serde.DaggerInternalTypeInformation;

import java.io.Serializable;

public class JsonType implements Serializable, DaggerInternalTypeInformation {
    private String jsonSchema;
    private String rowtimeAttributeName;

    public JsonType(String jsonSchema, String rowtimeAttributeName) {
        this.jsonSchema = jsonSchema;
        this.rowtimeAttributeName = rowtimeAttributeName;
    }

    public TypeInformation<Row> getRowType() {
        TypeInformation<Row> rowNamed = JsonRowSchemaConverter.convert(jsonSchema);

        return addInternalFields(rowNamed, rowtimeAttributeName);
    }
}
