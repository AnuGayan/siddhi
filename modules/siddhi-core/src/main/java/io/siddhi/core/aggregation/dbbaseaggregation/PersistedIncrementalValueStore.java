package io.siddhi.core.aggregation.dbbaseaggregation;

import io.siddhi.core.aggregation.IncrementalValueStore;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.selector.attribute.aggregator.incremental.IncrementalAttributeAggregator;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.aggregation.TimePeriod;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * IncrementalValueStore implementation for the persisted aggregation
 **/
public class PersistedIncrementalValueStore implements IncrementalValueStore {
    private static final Logger log = Logger.getLogger(PersistedIncrementalValueStore.class);
    List<IncrementalAttributeAggregator> incrementalAttributeAggregators;
    private StateHolder<ValueState> valueStateHolder;
    private StateHolder<StoreState> storeStateHolder;
    private long initialTimestamp;
    private List<ExpressionExecutor> expressionExecutors;
    private ExpressionExecutor shouldUpdateTimestamp;
    private StreamEventFactory streamEventFactory;
    private TimePeriod.Duration duration;

    public PersistedIncrementalValueStore(String aggregatorName, long initialTimestamp,
                                          List<ExpressionExecutor> expressionExecutors,
                                          List<IncrementalAttributeAggregator> incrementalAttributeAggregators,
                                          ExpressionExecutor shouldUpdateTimestamp,
                                          StreamEventFactory streamEventFactory, SiddhiQueryContext siddhiQueryContext,
                                          boolean groupBy, boolean local, TimePeriod.Duration duration) {


        this.initialTimestamp = initialTimestamp;
        this.expressionExecutors = expressionExecutors;
        this.shouldUpdateTimestamp = shouldUpdateTimestamp;
        this.streamEventFactory = streamEventFactory;
        this.duration = duration;
        this.incrementalAttributeAggregators = incrementalAttributeAggregators;
    }

    public synchronized void clearValues(long startTimeOfNewAggregates, StreamEvent resetEvent) {
        this.initialTimestamp = startTimeOfNewAggregates;
        setTimestamp(startTimeOfNewAggregates);
        setProcessed(false);
        this.valueStateHolder.cleanGroupByStates();
    }

    public List<ExpressionExecutor> getExpressionExecutors() {
        return expressionExecutors;
    }

    public synchronized boolean isProcessed() {
        StoreState state = this.storeStateHolder.getState();
        try {
            return state.isProcessed;
        } finally {
            this.storeStateHolder.returnState(state);
        }
    }

    private void setProcessed(boolean isProcessed) {
        StoreState state = this.storeStateHolder.getState();
        try {
            state.isProcessed = isProcessed;
        } finally {
            this.storeStateHolder.returnState(state);
        }
    }

    private long getTimestamp() {
        StoreState state = this.storeStateHolder.getState();
        try {
            return state.timestamp;
        } finally {
            this.storeStateHolder.returnState(state);
        }
    }

    private void setTimestamp(long timestamp) {
        StoreState state = this.storeStateHolder.getState();
        try {
            state.timestamp = timestamp;
        } finally {
            this.storeStateHolder.returnState(state);
        }
    }

    public synchronized Map<String, StreamEvent> getGroupedByEvents() {
        Map<String, StreamEvent> groupedByEvents = new HashMap<>();

        if (isProcessed()) {
            Map<String, ValueState> baseIncrementalValueStoreMap = this.valueStateHolder.getAllGroupByStates();
            try {
                for (Map.Entry<String, ValueState> state : baseIncrementalValueStoreMap.entrySet()) {
                    StreamEvent streamEvent = streamEventFactory.newInstance();
                    long timestamp = getTimestamp();
                    streamEvent.setTimestamp(timestamp);
                    state.getValue().setValue(timestamp, 0);
                    streamEvent.setOutputData(state.getValue().values);
                    groupedByEvents.put(state.getKey(), streamEvent);
                }
            } finally {
                this.valueStateHolder.returnGroupByStates(baseIncrementalValueStoreMap);
            }
        }
        return groupedByEvents;
    }

    public synchronized void process(StreamEvent streamEvent) {
        ValueState state = valueStateHolder.getState();
        try {
            boolean shouldUpdate = true;
            if (shouldUpdateTimestamp != null) {
                shouldUpdate = shouldUpdate(shouldUpdateTimestamp.execute(streamEvent), state);
            }
            for (int i = 0; i < expressionExecutors.size(); i++) { // keeping timestamp value location as null
                ExpressionExecutor expressionExecutor = expressionExecutors.get(i);
                if (shouldUpdate) {
                    state.setValue(expressionExecutor.execute(streamEvent), i + 1);
                } else if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
                    state.setValue(expressionExecutor.execute(streamEvent), i + 1);
                }
            }
            setProcessed(true);
        } finally {
            valueStateHolder.returnState(state);
        }
    }

    public synchronized void process(Map<String, StreamEvent> groupedByEvents) {
        for (Map.Entry<String, StreamEvent> eventEntry : groupedByEvents.entrySet()) {
            synchronized (this) {
                SiddhiAppContext.startGroupByFlow(eventEntry.getKey() + "-" +
                        eventEntry.getValue().getTimestamp());
                ValueState state = valueStateHolder.getState();
                try {
                    boolean shouldUpdate = true;
                    if (shouldUpdateTimestamp != null) {
                        shouldUpdate = shouldUpdate(shouldUpdateTimestamp.execute(eventEntry.getValue()), state);
                    }
                    for (int i = 0; i < expressionExecutors.size(); i++) { // keeping timestamp value location as null
                        ExpressionExecutor expressionExecutor = expressionExecutors.get(i);
                        if (shouldUpdate) {
                            state.setValue(expressionExecutor.execute(eventEntry.getValue()), i + 1);
                        } else if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
                            state.setValue(expressionExecutor.execute(eventEntry.getValue()), i + 1);
                        }
                    }
                    setProcessed(true);
                } finally {
                    valueStateHolder.returnState(state);
                    SiddhiAppContext.stopGroupByFlow();
                }
            }
        }
    }

    private boolean shouldUpdate(Object data, ValueState state) {
        long timestamp = (long) data;
        if (timestamp >= state.lastTimestamp) {
            state.lastTimestamp = timestamp;
            return true;
        }
        return false;
    }

    class StoreState extends State {
        private long timestamp;
        private boolean isProcessed = false;

        public StoreState() {
            this.timestamp = initialTimestamp;
        }

        @Override
        public boolean canDestroy() {
            return !isProcessed;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Timestamp", timestamp);
            state.put("IsProcessed", isProcessed);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            timestamp = (long) state.get("Timestamp");
            isProcessed = (boolean) state.get("IsProcessed");
        }

        public void setIfAbsentTimestamp(long timestamp) {
            if (this.timestamp == -1) {
                this.timestamp = timestamp;
            }
        }
    }

    class ValueState extends State {
        public long lastTimestamp = 0;
        private Object[] values;

        public ValueState() {
            this.values = new Object[expressionExecutors.size() + 1];
        }

        @Override
        public boolean canDestroy() {
            return values == null && lastTimestamp == 0;
        }

        public void setValue(Object value, int position) {
            values[position] = value;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Values", values);
            state.put("LastTimestamp", lastTimestamp);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            values = (Object[]) state.get("Values");
            lastTimestamp = (Long) state.get("LastTimestamp");
        }

    }

}
