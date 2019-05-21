package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;

public interface PostProcessor {
    StreamInfo process(StreamInfo streamInfo);
}
