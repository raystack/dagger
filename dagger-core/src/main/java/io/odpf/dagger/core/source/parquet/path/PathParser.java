package io.odpf.dagger.core.source.parquet.path;

import org.apache.flink.core.fs.Path;

import java.text.ParseException;
import java.time.Instant;

public interface PathParser {

    Instant instantFromFilePath(Path path) throws ParseException;
}
