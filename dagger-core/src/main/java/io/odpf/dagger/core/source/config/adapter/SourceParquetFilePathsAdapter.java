package io.odpf.dagger.core.source.config.adapter;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Arrays;

public class SourceParquetFilePathsAdapter extends TypeAdapter<String[]> {
    @Override
    public void write(JsonWriter jsonWriter, String[] strings) {
    }

    @Override
    public String[] read(JsonReader jsonReader) throws IOException {
        Gson gson = new Gson();
        String[] filePathArray = gson.fromJson(jsonReader, String[].class);
        return Arrays.stream(filePathArray)
                .map(String::valueOf)
                .map(String::trim).toArray(String[]::new);
    }
}
