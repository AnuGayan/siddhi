/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.table;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.table.record.RecordTableHandler;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * common interface for FIFO, LRU, and LFU cache tables
 */
public abstract class CacheTable extends InMemoryTable {
    private int maxSize;
    boolean cacheExpiryEnabled;
    SiddhiAppContext siddhiAppContext;

    public void initCacheTable(TableDefinition cacheTableDefinition, ConfigReader configReader,
                               SiddhiAppContext siddhiAppContext, RecordTableHandler recordTableHandler,
                               boolean cacheExpiryEnabled, int maxSize, String cachePolicy) {
        this.maxSize = maxSize;
        this.cacheExpiryEnabled = cacheExpiryEnabled;
        this.siddhiAppContext = siddhiAppContext;
        addRequiredFieldsToCacheTableDefinition(cacheTableDefinition, cacheExpiryEnabled);

        // initialize cache table
        MetaStreamEvent cacheTableMetaStreamEvent = new MetaStreamEvent();
        cacheTableMetaStreamEvent.addInputDefinition(cacheTableDefinition);
        for (Attribute attribute : cacheTableDefinition.getAttributeList()) {
            cacheTableMetaStreamEvent.addOutputData(attribute);
        }

        StreamEventFactory cacheTableStreamEventFactory = new StreamEventFactory(cacheTableMetaStreamEvent);
        StreamEventCloner cacheTableStreamEventCloner = new StreamEventCloner(cacheTableMetaStreamEvent,
                cacheTableStreamEventFactory);
        super.initTable(cacheTableDefinition, cacheTableStreamEventFactory,
                cacheTableStreamEventCloner, configReader, siddhiAppContext, recordTableHandler);
    }

    public void addStreamEventUptoMaxSize(StreamEvent streamEvent) {
        int sizeAfterAdding = 0;
        ComplexEventChunk<StreamEvent> addEventsLimitCopy = new ComplexEventChunk<>(true);
        StreamEvent streamEventIter;
        while (true) {
            sizeAfterAdding++;
            streamEventIter = streamEvent.getNext();
            if (sizeAfterAdding == maxSize || streamEventIter.getNext() == null) {
                streamEventIter.setNext(null);
                break;
            }
        }
        addEventsLimitCopy.add(streamEvent);
        readWriteLock.writeLock().lock();
        try {
            this.add(addEventsLimitCopy);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void addAndTrimUptoMaxSize(ComplexEventChunk<StreamEvent> addingEventChunk) {
        ComplexEventChunk<StreamEvent> addingEventChunkForCache = new ComplexEventChunk<>(true);
        addingEventChunk.reset();
        while (addingEventChunk.hasNext()) {
            StreamEvent event = addingEventChunk.next();
            addingEventChunkForCache.add((StreamEvent) generateEventWithRequiredFields(event, siddhiAppContext,
                    cacheExpiryEnabled));
        }
        readWriteLock.writeLock().lock();
        try {
            super.add(addingEventChunkForCache);
            while (this.size() > maxSize) {
                this.deleteOneEntryUsingCachePolicy();
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void updateOrAddWithMaxSize(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                                       CompiledCondition compiledCondition,
                                       CompiledUpdateSet compiledUpdateSet,
                                       AddingStreamEventExtractor addingStreamEventExtractor, int maxTableSize) {
        ComplexEventChunk<StateEvent> updateOrAddingEventChunkForCache = new ComplexEventChunk<>(true);
        updateOrAddingEventChunk.reset();
        while (updateOrAddingEventChunk.hasNext()) {
            StateEvent event = updateOrAddingEventChunk.next();
            updateOrAddingEventChunkForCache.add((StateEvent) generateEventWithRequiredFields(event, siddhiAppContext,
                    cacheExpiryEnabled));
        }

        readWriteLock.writeLock().lock();
        TableState state = stateHolder.getState();
        try {
            ComplexEventChunk<StreamEvent> failedEvents = ((Operator) compiledCondition).tryUpdate(
                    updateOrAddingEventChunkForCache,
                    state.getEventHolder(),
                    (InMemoryCompiledUpdateSet) compiledUpdateSet,
                    addingStreamEventExtractor);
            if (failedEvents != null && failedEvents.getFirst() != null) {
                state.getEventHolder().add(failedEvents);
            }
            while (this.size() > maxSize) {
                this.deleteOneEntryUsingCachePolicy();
            }
        } finally {
            stateHolder.returnState(state);
            readWriteLock.writeLock().unlock();
        }
    }

    public void deleteAll() {
        stateHolder.getState().getEventHolder().deleteAll();
    }

    abstract void addRequiredFieldsToCacheTableDefinition(TableDefinition cacheTableDefinition,
                                                          boolean cacheExpiryEnabled);

    public abstract void deleteOneEntryUsingCachePolicy();

    public ComplexEvent generateEventWithRequiredFields(Object event,
                                                        SiddhiAppContext siddhiAppContext,
                                                        boolean cacheExpiryEnabled) {
        if (event instanceof StreamEvent) {
            StreamEvent eventForCache = checkPolicyAndAddFields(event, siddhiAppContext, cacheExpiryEnabled);
            return eventForCache;
        } else if (event instanceof StateEvent) {
            StreamEvent eventForCache = checkPolicyAndAddFields(((StateEvent) event).getStreamEvent(0),
                    siddhiAppContext, cacheExpiryEnabled);
            StateEvent stateEvent = new StateEvent(((StateEvent) event).getStreamEvents().length,
                    eventForCache.getOutputData().length);
            stateEvent.addEvent(0, eventForCache);
            return stateEvent;
        } else {
            return null;
        }

    }

    protected abstract StreamEvent checkPolicyAndAddFields(Object event, SiddhiAppContext siddhiAppContext,
                                                           boolean cacheExpiryEnabled);

    public CompiledCondition generateCacheCompileCondition(Expression condition,
                                                            MatchingMetaInfoHolder storeMatchingMetaInfoHolder,
                                                            SiddhiQueryContext siddhiQueryContext,
                                                            List<VariableExpressionExecutor>
                                                                    storeVariableExpressionExecutors) {
        MetaStateEvent metaStateEvent = new MetaStateEvent(storeMatchingMetaInfoHolder.getMetaStateEvent().
                getMetaStreamEvents().length);
        for (MetaStreamEvent referenceMetaStreamEvent: storeMatchingMetaInfoHolder.getMetaStateEvent().
                getMetaStreamEvents()) {
            metaStateEvent.addEvent(referenceMetaStreamEvent);
        }
        MatchingMetaInfoHolder matchingMetaInfoHolder = new MatchingMetaInfoHolder(
                metaStateEvent,
                storeMatchingMetaInfoHolder.getMatchingStreamEventIndex(),
                storeMatchingMetaInfoHolder.getStoreEventIndex(),
                storeMatchingMetaInfoHolder.getMatchingStreamDefinition(),
                this.tableDefinition,
                storeMatchingMetaInfoHolder.getCurrentState());

        Map<String, Table> tableMap = new ConcurrentHashMap<>();
        tableMap.put(this.tableDefinition.getId(), this);

        return super.compileCondition(condition, matchingMetaInfoHolder,
                storeVariableExpressionExecutors, tableMap, siddhiQueryContext);
    }
}
