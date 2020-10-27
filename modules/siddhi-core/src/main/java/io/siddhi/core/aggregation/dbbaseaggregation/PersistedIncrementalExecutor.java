package io.siddhi.core.aggregation.dbbaseaggregation;

import io.siddhi.core.aggregation.Executor;
import io.siddhi.core.aggregation.IncrementalValueStore;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.query.selector.attribute.aggregator.incremental.IncrementalAttributeAggregator;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.IncrementalTimeConverterUtil;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.parser.AggregationParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.aggregation.TimePeriod;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Incremental Executor implementation class for Persisted Aggregation
 **/
public class PersistedIncrementalExecutor implements Executor {
    private static final Logger LOG = Logger.getLogger(PersistedIncrementalExecutor.class);

    private final String aggregatorName;
    private final StreamEvent resetEvent;
    private final ExpressionExecutor timestampExpressionExecutor;
    private final StateHolder<ExecutorState> stateHolder;
    private final String siddhiAppName;
    private TimePeriod.Duration duration;
    private Table table;
    private boolean isRoot;
    private Executor next;
    private GroupByKeyGenerator groupByKeyGenerator;
    private StreamEventFactory streamEventFactory;
    private Scheduler scheduler;
    private ExecutorService executorService;
    private String timeZone;
    private List<IncrementalAttributeAggregator> incrementalAttributeAggregators;
    private Processor cudStreamProcessor;
    private Table parentTable;
    private boolean isProcessingExecutor;

    private IncrementalValueStore baseIncrementalValueStore;

    public PersistedIncrementalExecutor(String aggregatorName, TimePeriod.Duration duration,
                                        List<ExpressionExecutor> processExpressionExecutors,
                                        ExpressionExecutor shouldUpdateTimestamp,
                                        GroupByKeyGenerator groupByKeyGenerator,
                                        List<IncrementalAttributeAggregator> incrementalAttributeAggregators,
                                        boolean isRoot, Table table, Executor child,
                                        SiddhiQueryContext siddhiQueryContext, MetaStreamEvent metaStreamEvent,
                                        String timeZone, Processor cudStreamProcessor) {
        this.timeZone = timeZone;
        this.aggregatorName = aggregatorName;
        this.duration = duration;
        this.isRoot = isRoot;
        this.table = table;
        this.next = child;
        this.incrementalAttributeAggregators = incrementalAttributeAggregators;
        this.cudStreamProcessor = cudStreamProcessor;

        this.timestampExpressionExecutor = processExpressionExecutors.remove(0);
        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);

        this.groupByKeyGenerator = groupByKeyGenerator;
        this.baseIncrementalValueStore = new PersistedIncrementalValueStore(aggregatorName, -1,
                processExpressionExecutors, incrementalAttributeAggregators, shouldUpdateTimestamp, streamEventFactory,
                siddhiQueryContext, true, false, duration);
        this.resetEvent = AggregationParser.createRestEvent(metaStreamEvent, streamEventFactory.newInstance());
        setNextExecutor(child);

        this.siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
        this.stateHolder = siddhiQueryContext.generateStateHolder(
                aggregatorName + "-" + this.getClass().getName(), false,
                () -> new ExecutorState());
        this.executorService = Executors.newSingleThreadExecutor();

        this.isProcessingExecutor = false;
    }

    @Override
    public void execute(ComplexEventChunk streamEventChunk) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Event Chunk received by " + this.duration + " incremental executor: " +
                    streamEventChunk.toString() + " will be dropped since persisted aggregation has been scheduled ");
        }
        streamEventChunk.reset();
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = (StreamEvent) streamEventChunk.next();
            streamEventChunk.remove();
            ExecutorState executorState = stateHolder.getState();
            try {
                long timestamp = getTimestamp(streamEvent);
                if (timestamp >= executorState.nextEmitTime) {
                    long emittedTime = executorState.nextEmitTime;
                    long startedTime = executorState.startTimeOfAggregates;
                    executorState.startTimeOfAggregates = IncrementalTimeConverterUtil.getStartTimeOfAggregates(
                            timestamp, duration, timeZone);
                    executorState.nextEmitTime = IncrementalTimeConverterUtil.getNextEmitTime(
                            timestamp, duration, timeZone);
                    dispatchAggregateEvents(startedTime, emittedTime);
                    sendTimerEvent(executorState);
                }
            } finally {
                stateHolder.returnState(executorState);
            }
        }
    }

    private void dispatchAggregateEvents(long startTimeOfNewAggregates, long emittedTime) {
        if (emittedTime != -1) {
            dispatchEvent(startTimeOfNewAggregates, emittedTime, baseIncrementalValueStore);
        }
    }

    private void dispatchEvent(long startTimeOfNewAggregates, long emittedTime,
                               IncrementalValueStore aBaseIncrementalValueStore) {
        LOG.info("Aggragation startTime " + startTimeOfNewAggregates + " EmittedTime " + emittedTime);
        ComplexEventChunk complexEventChunk = new ComplexEventChunk();
        StreamEvent streamEvent = streamEventFactory.newInstance();
        streamEvent.setType(ComplexEvent.Type.CURRENT);
        streamEvent.setTimestamp(emittedTime);
        List<Object> outputDataList = new ArrayList<>();
        outputDataList.add(startTimeOfNewAggregates);
        outputDataList.add(startTimeOfNewAggregates);
        outputDataList.add(emittedTime);
        outputDataList.add(null);
        streamEvent.setOutputData(outputDataList.toArray());
        streamEvent.setBeforeWindowData(null);
        complexEventChunk.add(streamEvent);
        cudStreamProcessor.process(complexEventChunk);
        if (getNextExecutor() != null) {
            next.execute(complexEventChunk);
        }
    }

    IncrementalValueStore getBaseIncrementalValueStore() {
        return baseIncrementalValueStore;
    }

    public long getAggregationStartTimestamp() {
        ExecutorState state = stateHolder.getState();
        try {
            return state.startTimeOfAggregates;
        } finally {
            stateHolder.returnState(state);
        }
    }

    public void setEmitTime(long emitTimeOfLatestEventInTable) {
        ExecutorState state = stateHolder.getState();
        try {
            state.nextEmitTime = emitTimeOfLatestEventInTable;
        } finally {
            stateHolder.returnState(state);
        }
    }

    public void setProcessingExecutor(boolean processingExecutor) {
        isProcessingExecutor = processingExecutor;
    }

    private void sendTimerEvent(ExecutorState executorState) {
        if (getNextExecutor() != null) {
            StreamEvent timerEvent = streamEventFactory.newInstance();
            timerEvent.setType(ComplexEvent.Type.TIMER);
            timerEvent.setTimestamp(executorState.startTimeOfAggregates);
            ComplexEventChunk<StreamEvent> timerStreamEventChunk = new ComplexEventChunk<>();
            timerStreamEventChunk.add(timerEvent);
            next.execute(timerStreamEventChunk);
        }
    }

    private long getTimestamp(StreamEvent streamEvent) {
        long timestamp;
        if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
            timestamp = (long) timestampExpressionExecutor.execute(streamEvent);
        } else {
            timestamp = streamEvent.getTimestamp();
            if (isRoot) {
                // Scheduling is done by root incremental executor only
                scheduler.notifyAt(IncrementalTimeConverterUtil.getNextEmitTime(timestamp, duration, timeZone));
            }
        }
        return timestamp;
    }

    @Override
    public Executor getNextExecutor() {
        return null;
    }

    @Override
    public void setNextExecutor(Executor executor) {

    }

    private void processAggregates(StreamEvent streamEvent, ExecutorState executorState) {
        synchronized (this) {
            if (groupByKeyGenerator != null) {
                try {
                    String groupedByKey = groupByKeyGenerator.constructEventKey(streamEvent);
                    SiddhiAppContext.startGroupByFlow(groupedByKey);
                    baseIncrementalValueStore.process(streamEvent);
                } finally {
                    SiddhiAppContext.stopGroupByFlow();
                }
            } else {
                baseIncrementalValueStore.process(streamEvent);
            }
        }
    }

    class ExecutorState extends State {
        private long nextEmitTime = -1;
        private long startTimeOfAggregates = -1;
        private boolean timerStarted = false;
        private boolean canDestroy = false;

        @Override
        public boolean canDestroy() {
            return canDestroy;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("NextEmitTime", nextEmitTime);
            state.put("StartTimeOfAggregates", startTimeOfAggregates);
            state.put("TimerStarted", timerStarted);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            nextEmitTime = (long) state.get("NextEmitTime");
            startTimeOfAggregates = (long) state.get("StartTimeOfAggregates");
            timerStarted = (boolean) state.get("TimerStarted");
        }

        public void setCanDestroy(boolean canDestroy) {
            this.canDestroy = canDestroy;
        }
    }
}
