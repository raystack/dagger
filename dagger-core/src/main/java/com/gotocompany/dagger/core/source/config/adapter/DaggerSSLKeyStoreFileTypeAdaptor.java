package com.gotocompany.dagger.core.source.config.adapter;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.gotocompany.dagger.core.exception.InvalidConfigurationException;
import com.gotocompany.dagger.core.utils.Constants;

import java.io.IOException;
import java.util.Arrays;

public class DaggerSSLKeyStoreFileTypeAdaptor extends TypeAdapter<String> {
    @Override
    public void write(JsonWriter jsonWriter, String value) throws IOException {
        if (value == null) {
            jsonWriter.nullValue();
            return;
        }
        jsonWriter.value(value);
    }

    @Override
    public String read(JsonReader jsonReader) throws IOException {
        String keyStoreFileType = jsonReader.nextString();
        if (Arrays.stream(Constants.SUPPORTED_SOURCE_KAFKA_CONSUMER_CONFIG_SSL_STORE_FILE_TYPE).anyMatch(keyStoreFileType::equals)) {
            return keyStoreFileType;
        } else {
            throw new InvalidConfigurationException(String.format("Configured wrong SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_TYPE_KEY supported values are %s", Arrays.toString(Constants.SUPPORTED_SOURCE_KAFKA_CONSUMER_CONFIG_SSL_STORE_FILE_TYPE)));
        }
    }
}
