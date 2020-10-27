package io.siddhi.core.aggregation;

import io.siddhi.core.event.stream.StreamEvent;

/**
 * Interface class for the Aggregation executors
 * **/
public interface IncrementalValueStore {
    void process(StreamEvent streamEvent);
}
