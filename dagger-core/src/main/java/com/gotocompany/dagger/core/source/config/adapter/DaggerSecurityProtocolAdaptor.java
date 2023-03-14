package com.gotocompany.dagger.core.source.config.adapter;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.gotocompany.dagger.core.exception.InvalidConfigurationException;
import com.gotocompany.dagger.core.utils.Constants;

import java.io.IOException;
import java.util.Arrays;

public class DaggerSecurityProtocolAdaptor extends TypeAdapter<String> {
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
        String securityProtocol = jsonReader.nextString();
        if (Arrays.stream(Constants.SUPPORTED_SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL).anyMatch(securityProtocol::equals)) {
            return securityProtocol;
        } else {
            throw new InvalidConfigurationException(String.format("Configured wrong SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL supported values are %s", Arrays.toString(Constants.SUPPORTED_SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL)));
        }
    }
}
