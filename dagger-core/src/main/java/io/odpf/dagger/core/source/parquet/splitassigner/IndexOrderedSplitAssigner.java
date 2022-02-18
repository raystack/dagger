package io.odpf.dagger.core.source.parquet.splitassigner;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;

/* TODO */
public class IndexOrderedSplitAssigner implements FileSplitAssigner {

    public IndexOrderedSplitAssigner(Collection<FileSourceSplit> fileSourceSplits) {
    }

    @Override
    public Optional<FileSourceSplit> getNext(@Nullable String hostname) {
        return Optional.empty();
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> splits) {

    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        return null;
    }
}
