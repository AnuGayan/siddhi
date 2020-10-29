/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.util.parser;

import io.siddhi.core.aggregation.AggregationRuntime;
import io.siddhi.core.aggregation.Executor;
import io.siddhi.core.aggregation.IncrementalAggregationProcessor;
import io.siddhi.core.aggregation.IncrementalDataPurger;
import io.siddhi.core.aggregation.IncrementalExecutor;
import io.siddhi.core.aggregation.IncrementalExecutorsInitialiser;
import io.siddhi.core.aggregation.dbbaseaggregation.PersistedIncrementalExecutor;
import io.siddhi.core.aggregation.dbbaseaggregation.config.CUDDataProcessor;
import io.siddhi.core.aggregation.dbbaseaggregation.config.DBAggregationQueryConfigurationEntry;
import io.siddhi.core.aggregation.dbbaseaggregation.config.DBAggregationQueryUtil;
import io.siddhi.core.aggregation.dbbaseaggregation.config.DBAggregationSelectFunctionTemplate;
import io.siddhi.core.aggregation.dbbaseaggregation.config.DBAggregationSelectQueryTemplate;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.CannotLoadConfigurationException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.executor.incremental.IncrementalAggregateBaseTimeFunctionExecutor;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.query.input.stream.single.EntryValveExecutor;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.AbstractStreamProcessor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.query.selector.attribute.aggregator.MaxAttributeAggregatorExecutor;
import io.siddhi.core.query.selector.attribute.aggregator.SumAttributeAggregatorExecutor;
import io.siddhi.core.query.selector.attribute.aggregator.incremental.IncrementalAttributeAggregator;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.SiddhiAppRuntimeBuilder;
import io.siddhi.core.util.SiddhiClassLoader;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigManager;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.extension.holder.FunctionExecutorExtensionHolder;
import io.siddhi.core.util.extension.holder.IncrementalAttributeAggregatorExtensionHolder;
import io.siddhi.core.util.extension.holder.StreamProcessorExtensionHolder;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.AggregationDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.input.handler.StreamFunction;
import io.siddhi.query.api.execution.query.input.handler.StreamHandler;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.AttributeFunction;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.constant.StringConstant;
import io.siddhi.query.api.extension.Extension;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static io.siddhi.core.util.SiddhiConstants.AGG_EXTERNAL_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_LAST_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_SHARD_ID_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_START_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_PARTITION_BY_ID;
import static io.siddhi.core.util.SiddhiConstants.FROM_TIMESTAMP;
import static io.siddhi.core.util.SiddhiConstants.METRIC_INFIX_AGGREGATIONS;
import static io.siddhi.core.util.SiddhiConstants.METRIC_TYPE_FIND;
import static io.siddhi.core.util.SiddhiConstants.METRIC_TYPE_INSERT;
import static io.siddhi.core.util.SiddhiConstants.PLACEHOLDER_COLUMN;
import static io.siddhi.core.util.SiddhiConstants.PLACEHOLDER_COLUMNS;
import static io.siddhi.core.util.SiddhiConstants.PLACEHOLDER_CONDITION;
import static io.siddhi.core.util.SiddhiConstants.PLACEHOLDER_SELECTORS;
import static io.siddhi.core.util.SiddhiConstants.PLACEHOLDER_TABLE_NAME;
import static io.siddhi.core.util.SiddhiConstants.TO_TIMESTAMP;
import static io.siddhi.core.util.SiddhiConstants.UPDATED_TIMESTAMP;

/**
 * This is the parser class of incremental aggregation definition.
 */
public class AggregationParser {
    private static final Logger log = Logger.getLogger(AggregationParser.class);
    public static Map<String,Map<TimePeriod.Duration,Executor>> aggregationDurationExecutorMap = new HashMap<>();

    public static AggregationRuntime parse(AggregationDefinition aggregationDefinition,
                                           SiddhiAppContext siddhiAppContext,
                                           Map<String, AbstractDefinition> streamDefinitionMap,
                                           Map<String, AbstractDefinition> tableDefinitionMap,
                                           Map<String, AbstractDefinition> windowDefinitionMap,
                                           Map<String, AbstractDefinition> aggregationDefinitionMap,
                                           Map<String, Table> tableMap,
                                           Map<String, Window> windowMap,
                                           Map<String, AggregationRuntime> aggregationMap,
                                           SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder) {
        //set timeZone for aggregation
        String timeZone = getTimeZone(siddhiAppContext);
        boolean isDebugEnabled = log.isDebugEnabled();
        boolean isPersistedAggregation = false;
        if (!validateTimeZone(timeZone)) {
            throw new SiddhiAppCreationException(
                    "Given timeZone '" + timeZone + "' for aggregations is invalid. Please provide a valid time zone."
            );
        }
        //get aggregation name
        String aggregatorName = aggregationDefinition.getId();

        if (isDebugEnabled) {
            log.debug("Incremental aggregation initialization process started for aggregation "
                    + aggregatorName);
        }
        Annotation aggregationProperties = AnnotationHelper.getAnnotation("aggregationProperties",
                aggregationDefinition.getAnnotations());
        String aggregationMode = "inmemory";
        if (aggregationProperties != null) {
            aggregationMode = aggregationProperties.getElement("mode");
        }
        if (aggregationMode.equals("persisted")) {
            isPersistedAggregation = true;
        }
        if (isDebugEnabled) {
            log.debug("Aggregation mode is defined as " + aggregationMode + " for aggregation "
                    + aggregatorName);
        }
        if (aggregationDefinition.getTimePeriod() == null) {
            throw new SiddhiAppCreationException(
                    "Aggregation Definition '" + aggregationDefinition.getId() + "'s timePeriod is null. " +
                            "Hence, can't create the siddhi app '" + siddhiAppContext.getName() + "'",
                    aggregationDefinition.getQueryContextStartIndex(), aggregationDefinition.getQueryContextEndIndex());
        }
        if (aggregationDefinition.getSelector() == null) {
            throw new SiddhiAppCreationException(
                    "Aggregation Definition '" + aggregationDefinition.getId() + "'s selection is not defined. " +
                            "Hence, can't create the siddhi app '" + siddhiAppContext.getName() + "'",
                    aggregationDefinition.getQueryContextStartIndex(), aggregationDefinition.getQueryContextEndIndex());
        }
        if (streamDefinitionMap.get(aggregationDefinition.getBasicSingleInputStream().getStreamId()) == null) {
            throw new SiddhiAppCreationException("Stream " + aggregationDefinition.getBasicSingleInputStream().
                    getStreamId() + " has not been defined");
        }

        //Check if the user defined primary keys exists for @store annotation on aggregation if yes, an error is
        //is thrown
        Element userDefinedPrimaryKey = AnnotationHelper.getAnnotationElement(
                SiddhiConstants.ANNOTATION_PRIMARY_KEY, null, aggregationDefinition.getAnnotations());
        if (userDefinedPrimaryKey != null) {
            throw new SiddhiAppCreationException("Aggregation Tables have predefined primary key, but found '" +
                    userDefinedPrimaryKey.getValue() + "' primary key defined though annotation.");
        }

        try {
            List<VariableExpressionExecutor> incomingVariableExpressionExecutors = new ArrayList<>();

            SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, aggregatorName);

            StreamRuntime streamRuntime = InputStreamParser.parse(aggregationDefinition.getBasicSingleInputStream(),
                    null, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, aggregationDefinitionMap,
                    tableMap, windowMap, aggregationMap, incomingVariableExpressionExecutors, false,
                    siddhiQueryContext);

            // Get original meta for later use.
            MetaStreamEvent incomingMetaStreamEvent = (MetaStreamEvent) streamRuntime.getMetaComplexEvent();
            // Create new meta stream event.
            // This must hold the timestamp, group by attributes (if given) and the incremental attributes, in
            // onAfterWindowData array
            // Example format: AGG_TIMESTAMP, groupByAttribute1, groupByAttribute2, AGG_incAttribute1, AGG_incAttribute2
            // AGG_incAttribute1, AGG_incAttribute2 would have the same attribute names as in
            // finalListOfIncrementalAttributes
            incomingMetaStreamEvent.initializeOnAfterWindowData(); // To enter data as onAfterWindowData
            //List of all aggregationDurations
            List<TimePeriod.Duration> activeIncrementalDurations = getSortedPeriods(aggregationDefinition.getTimePeriod());
            //get aggegation durations
//            List<TimePeriod.Duration> incrementalDurations = ;
            List<TimePeriod.Duration> incrementalDurations = Arrays.asList(TimePeriod.Duration.values().clone());


            //Incoming executors will be executors for timestamp, externalTimestamp(if used),
            List<ExpressionExecutor> incomingExpressionExecutors = new ArrayList<>();
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators = new ArrayList<>();

            // group by attributes (if given) and the incremental attributes expression executors
            List<Variable> groupByVariableList = aggregationDefinition.getSelector().getGroupByList();

            //Expressions to get final aggregate outputs. e.g for avg the expression is Divide expression with
            // AGG_SUM/ AGG_COUNT
            List<Expression> outputExpressions = new ArrayList<>();

            boolean isProcessingOnExternalTime = aggregationDefinition.getAggregateAttribute() != null;
            boolean isGroupBy = aggregationDefinition.getSelector().getGroupByList().size() != 0;


            final boolean isDistributed;
            ConfigManager configManager = siddhiAppContext.getSiddhiContext().getConfigManager();
            final String shardId = configManager.extractProperty("shardId");
            boolean enablePartitioning = false;
            // check if the setup is Active Active(distributed deployment) by checking availability of partitionById
            // config
            Annotation partitionById = AnnotationHelper.getAnnotation(ANNOTATION_PARTITION_BY_ID,
                    aggregationDefinition.getAnnotations());
            if (partitionById != null) {
                String enableElement = partitionById.getElement("enable");
                enablePartitioning = enableElement == null || Boolean.parseBoolean(enableElement);
            }

            boolean shouldPartitionById = Boolean.parseBoolean(configManager.extractProperty("partitionById"));

            if (enablePartitioning || shouldPartitionById) {
                if (shardId == null) {
                    throw new SiddhiAppCreationException("Configuration 'shardId' not provided for @partitionById " +
                            "annotation");
                }
                isDistributed = true;
            } else {
                isDistributed = false;
            }

            if (isDebugEnabled) {
                log.debug("Distributed aggregation processing is " + (isDistributed ? "enabled" : "disabled") +
                        " in " + aggregatorName + " aggregation ");
            }

            populateIncomingAggregatorsAndExecutors(aggregationDefinition, siddhiQueryContext, tableMap,
                    incomingVariableExpressionExecutors, incomingMetaStreamEvent, incomingExpressionExecutors,
                    incrementalAttributeAggregators, groupByVariableList, outputExpressions, isProcessingOnExternalTime,
                    isDistributed, shardId);

            // check if the populateIncomingAggregatorsAndExecutors process has been completed successfully
            boolean isLatestEventColAdded = incomingMetaStreamEvent.getOutputData()
                    .get(incomingMetaStreamEvent.getOutputData().size() - 1)
                    .getName().equals(AGG_LAST_TIMESTAMP_COL);


            int baseAggregatorBeginIndex = incomingMetaStreamEvent.getOutputData().size();

            List<Expression> finalBaseExpressions = new ArrayList<>();
            boolean isOptimisedLookup = populateFinalBaseAggregators(tableMap, incomingVariableExpressionExecutors,
                    incomingMetaStreamEvent, incomingExpressionExecutors, incrementalAttributeAggregators,
                    siddhiQueryContext, finalBaseExpressions);

            if (isDebugEnabled) {
                log.debug("Optimised lookup mode is " + (isOptimisedLookup ? "enabled" : "disabled") + " for " +
                        "aggregation " + aggregatorName);
            }

            // Creating an intermediate stream with aggregated stream and above extracted output variables
            StreamDefinition incomingOutputStreamDefinition = StreamDefinition.id(aggregatorName + "_intermediate");
            incomingOutputStreamDefinition.setQueryContextStartIndex(aggregationDefinition.getQueryContextStartIndex());
            incomingOutputStreamDefinition.setQueryContextEndIndex(aggregationDefinition.getQueryContextEndIndex());
            MetaStreamEvent processedMetaStreamEvent = new MetaStreamEvent();
            for (Attribute attribute : incomingMetaStreamEvent.getOutputData()) {
                incomingOutputStreamDefinition.attribute(attribute.getName(), attribute.getType());
                processedMetaStreamEvent.addOutputData(attribute);
            }
            incomingMetaStreamEvent.setOutputDefinition(incomingOutputStreamDefinition);
            processedMetaStreamEvent.addInputDefinition(incomingOutputStreamDefinition);
            processedMetaStreamEvent.setOutputDefinition(incomingOutputStreamDefinition);

            // Executors of processing meta
            List<VariableExpressionExecutor> processVariableExpressionExecutors = new ArrayList<>();

            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMap = new HashMap<>();
            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMapForFind = new HashMap<>();

            incrementalDurations.forEach(
                    incrementalDuration -> {
                        processExpressionExecutorsMap.put(
                                incrementalDuration,
                                constructProcessExpressionExecutors(
                                        siddhiQueryContext, tableMap, baseAggregatorBeginIndex, finalBaseExpressions,
                                        incomingOutputStreamDefinition, processedMetaStreamEvent,
                                        processVariableExpressionExecutors, isProcessingOnExternalTime,
                                        incrementalDuration, isDistributed, shardId, isLatestEventColAdded));
                        processExpressionExecutorsMapForFind.put(
                                incrementalDuration,
                                constructProcessExpressionExecutors(
                                        siddhiQueryContext, tableMap, baseAggregatorBeginIndex, finalBaseExpressions,
                                        incomingOutputStreamDefinition, processedMetaStreamEvent,
                                        processVariableExpressionExecutors, isProcessingOnExternalTime,
                                        incrementalDuration, isDistributed, shardId, isLatestEventColAdded));

                    });

            ExpressionExecutor shouldUpdateTimestamp = null;
            if (isLatestEventColAdded) {
                Expression shouldUpdateTimestampExp = new Variable(AGG_LAST_TIMESTAMP_COL);
                shouldUpdateTimestamp = ExpressionParser.parseExpression(shouldUpdateTimestampExp,
                        processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors,
                        false, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            }


            List<ExpressionExecutor> outputExpressionExecutors = outputExpressions.stream()
                    .map(expression -> ExpressionParser.parseExpression(expression, processedMetaStreamEvent, 0,
                            tableMap, processVariableExpressionExecutors, isGroupBy, 0, ProcessingMode.BATCH, false,
                            siddhiQueryContext))
                    .collect(Collectors.toList());

            // Create group by key generator
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMap = new HashMap<>();
            incrementalDurations.forEach(
                    incrementalDuration -> {
                        GroupByKeyGenerator groupByKeyGenerator = null;
                        if (isProcessingOnExternalTime || isGroupBy) {
                            List<Expression> groupByExpressionList = new ArrayList<>();
                            if (isProcessingOnExternalTime) {
                                Expression externalTimestampExpression =
                                        AttributeFunction.function(
                                                "incrementalAggregator", "getAggregationStartTime",
                                                new Variable(AGG_EXTERNAL_TIMESTAMP_COL),
                                                new StringConstant(incrementalDuration.name())
                                        );
                                groupByExpressionList.add(externalTimestampExpression);
                            }
                            groupByExpressionList.addAll(groupByVariableList.stream()
                                    .map(groupByVariable -> (Expression) groupByVariable)
                                    .collect(Collectors.toList()));
                            groupByKeyGenerator = new GroupByKeyGenerator(groupByExpressionList, processedMetaStreamEvent,
                                    SiddhiConstants.UNKNOWN_STATE, tableMap, processVariableExpressionExecutors,
                                    siddhiQueryContext);
                        }
                        groupByKeyGeneratorMap.put(incrementalDuration, groupByKeyGenerator);
                    }
            );

            // GroupBy for reading
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMapForReading = new HashMap<>();
            if (isDistributed && !isProcessingOnExternalTime) {
                incrementalDurations.forEach(
                        incrementalDuration -> {
                            List<Expression> groupByExpressionList = new ArrayList<>();
                            Expression timestampExpression =
                                    AttributeFunction.function(
                                            "incrementalAggregator", "getAggregationStartTime",
                                            new Variable(AGG_START_TIMESTAMP_COL),
                                            new StringConstant(incrementalDuration.name())
                                    );
                            groupByExpressionList.add(timestampExpression);
                            if (isGroupBy) {
                                groupByExpressionList.addAll(groupByVariableList.stream()
                                        .map(groupByVariable -> (Expression) groupByVariable)
                                        .collect(Collectors.toList()));
                            }
                            GroupByKeyGenerator groupByKeyGenerator = new GroupByKeyGenerator(groupByExpressionList,
                                    processedMetaStreamEvent, SiddhiConstants.UNKNOWN_STATE, tableMap,
                                    processVariableExpressionExecutors, siddhiQueryContext);
                            groupByKeyGeneratorMapForReading.put(incrementalDuration, groupByKeyGenerator);
                        }

                );

            } else {
                groupByKeyGeneratorMapForReading.putAll(groupByKeyGeneratorMap);
            }

            // Create new scheduler
            EntryValveExecutor entryValveExecutor = new EntryValveExecutor(siddhiAppContext);
            LockWrapper lockWrapper = new LockWrapper(aggregatorName);
            lockWrapper.setLock(new ReentrantLock());

            Scheduler scheduler = SchedulerParser.parse(entryValveExecutor, siddhiQueryContext);
            scheduler.init(lockWrapper, aggregatorName);
            scheduler.setStreamEventFactory(new StreamEventFactory(processedMetaStreamEvent));

            QueryParserHelper.reduceMetaComplexEvent(incomingMetaStreamEvent);
            QueryParserHelper.reduceMetaComplexEvent(processedMetaStreamEvent);
            QueryParserHelper.updateVariablePosition(incomingMetaStreamEvent, incomingVariableExpressionExecutors);
            QueryParserHelper.updateVariablePosition(processedMetaStreamEvent, processVariableExpressionExecutors);

            Map<TimePeriod.Duration, Table> aggregationTables = initDefaultTables(aggregatorName,
                    incrementalDurations, processedMetaStreamEvent.getOutputStreamDefinition(),
                    siddhiAppRuntimeBuilder, aggregationDefinition.getAnnotations(), groupByVariableList,
                    isProcessingOnExternalTime, isDistributed, activeIncrementalDurations);
            Map<TimePeriod.Duration, Processor> cudProcessors = null;

            if (isPersistedAggregation) {
                cudProcessors = initAggregateQueryExecutor(incrementalDurations, processExpressionExecutorsMap,
                        incomingOutputStreamDefinition, isDistributed, shardId, isProcessingOnExternalTime,
                        siddhiQueryContext, aggregationDefinition, configManager, aggregationTables,
                        activeIncrementalDurations);
            }

            Map<TimePeriod.Duration, Executor> incrementalExecutorMap = buildIncrementalExecutors(
                    processedMetaStreamEvent, processExpressionExecutorsMap, groupByKeyGeneratorMap,
                    incrementalDurations, aggregationTables, siddhiQueryContext,
                    aggregatorName, shouldUpdateTimestamp, timeZone, isPersistedAggregation, cudProcessors);

            aggregationDurationExecutorMap.put(aggregatorName,incrementalExecutorMap);

            isOptimisedLookup = isOptimisedLookup &&
                    aggregationTables.get(activeIncrementalDurations.get(0)) instanceof QueryableProcessor;

            List<String> groupByVariablesList = groupByVariableList.stream()
                    .map(Variable::getAttributeName)
                    .collect(Collectors.toList());

            List<OutputAttribute> defaultSelectorList = new ArrayList<>();
            if (isOptimisedLookup) {
                defaultSelectorList = incomingOutputStreamDefinition.getAttributeList().stream()
                        .map((attribute) -> new OutputAttribute(new Variable(attribute.getName())))
                        .collect(Collectors.toList());
            }

            IncrementalDataPurger incrementalDataPurger = new IncrementalDataPurger();
            incrementalDataPurger.init(aggregationDefinition, new StreamEventFactory(processedMetaStreamEvent)
                    , aggregationTables, isProcessingOnExternalTime, siddhiQueryContext, activeIncrementalDurations);

            //Recreate in-memory data from tables
            IncrementalExecutorsInitialiser incrementalExecutorsInitialiser = new IncrementalExecutorsInitialiser(
                    activeIncrementalDurations, aggregationTables, incrementalExecutorMap, isDistributed, shardId,
                    siddhiAppContext, processedMetaStreamEvent, tableMap, windowMap, aggregationMap, timeZone);

            IncrementalExecutor rootIncrementalExecutor = (IncrementalExecutor) incrementalExecutorMap.
                    get(incrementalDurations.get(0));
            rootIncrementalExecutor.setScheduler(scheduler);
            // Connect entry valve to root incremental executor
            entryValveExecutor.setNextExecutor(rootIncrementalExecutor);

            QueryParserHelper.initStreamRuntime(streamRuntime, incomingMetaStreamEvent, lockWrapper, aggregatorName);

            LatencyTracker latencyTrackerFind = null;
            LatencyTracker latencyTrackerInsert = null;

            ThroughputTracker throughputTrackerFind = null;
            ThroughputTracker throughputTrackerInsert = null;

            if (siddhiAppContext.getStatisticsManager() != null) {
                latencyTrackerFind = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_FIND);
                latencyTrackerInsert = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_INSERT);

                throughputTrackerFind = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_FIND);
                throughputTrackerInsert = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_INSERT);
            }

            AggregationRuntime aggregationRuntime = new AggregationRuntime(aggregationDefinition,
                    isProcessingOnExternalTime, isDistributed, incrementalDurations, activeIncrementalDurations,
                    incrementalExecutorMap, aggregationTables, outputExpressionExecutors,
                    processExpressionExecutorsMapForFind, shouldUpdateTimestamp, groupByKeyGeneratorMapForReading,
                    isOptimisedLookup, defaultSelectorList, groupByVariablesList, isLatestEventColAdded,
                    baseAggregatorBeginIndex, finalBaseExpressions, incrementalDataPurger,
                    incrementalExecutorsInitialiser, ((SingleStreamRuntime) streamRuntime), processedMetaStreamEvent,
                    latencyTrackerFind, throughputTrackerFind, timeZone);

            streamRuntime.setCommonProcessor(new IncrementalAggregationProcessor(aggregationRuntime,
                    incomingExpressionExecutors, processedMetaStreamEvent, latencyTrackerInsert,
                    throughputTrackerInsert, siddhiAppContext));

            return aggregationRuntime;

        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, aggregationDefinition, siddhiAppContext);
            throw t;
        }
    }

    private static String getTimeZone(SiddhiAppContext siddhiAppContext) {
        String timeZone = siddhiAppContext.getSiddhiContext().getConfigManager().extractProperty(SiddhiConstants
                .AGG_TIME_ZONE);
        if (timeZone == null) {
            return SiddhiConstants.AGG_TIME_ZONE_DEFAULT;
        }
        return timeZone;
    }

    private static Boolean validateTimeZone(String timeZone) {
        Set timeZoneSet = new HashSet(Arrays.asList(TimeZone.getAvailableIDs()));
        return timeZoneSet.contains(timeZone);
    }

    private static Map<TimePeriod.Duration, Executor> buildIncrementalExecutors(
            MetaStreamEvent processedMetaStreamEvent,
            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMap,
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorList,
            List<TimePeriod.Duration> incrementalDurations,
            Map<TimePeriod.Duration, Table> aggregationTables,
            SiddhiQueryContext siddhiQueryContext,
            String aggregatorName, ExpressionExecutor shouldUpdateTimestamp, String timeZone,
            boolean isPersistedAggregation, Map<TimePeriod.Duration, Processor> cudProcessors) {


        Map<TimePeriod.Duration, Executor> incrementalExecutorMap = new HashMap<>();
        // Create incremental executors
        Executor child;
        Executor root = null;

        if (isPersistedAggregation) {
            for (int i = incrementalDurations.size() - 1; i >= 0; i--) {
                // Base incremental expression executors created using new meta

                // Add an object to aggregationTable map inorder fill up the missing durations
                aggregationTables.putIfAbsent(incrementalDurations.get(i), null);
                boolean isRoot = false;
                if (i == 0) {
                    isRoot = true;
                }
                child = root;
                TimePeriod.Duration duration = incrementalDurations.get(i);
                Executor incrementalExecutor;
                if (duration == TimePeriod.Duration.SECONDS || duration == TimePeriod.Duration.MINUTES ||
                        duration == TimePeriod.Duration.HOURS) {
                    incrementalExecutor = new IncrementalExecutor(aggregatorName, duration,
                            processExpressionExecutorsMap.get(duration), shouldUpdateTimestamp,
                            groupByKeyGeneratorList.get(duration), isRoot, aggregationTables.get(duration),
                            child, siddhiQueryContext, processedMetaStreamEvent, timeZone);
                } else {
                    incrementalExecutor = new PersistedIncrementalExecutor(aggregatorName, duration,
                            processExpressionExecutorsMap.get(duration),
                            child, siddhiQueryContext, generateCUDMetaStreamEvent(), timeZone,
                            cudProcessors.get(duration));
                }
                incrementalExecutorMap.put(duration, incrementalExecutor);
                root = incrementalExecutor;

            }
        } else {
            for (int i = incrementalDurations.size() - 1; i >= 0; i--) {
                // Base incremental expression executors created using new meta
                boolean isRoot = false;
                if (i == 0) {
                    isRoot = true;
                }
                child = root;
                TimePeriod.Duration duration = incrementalDurations.get(i);

                IncrementalExecutor incrementalExecutor = new IncrementalExecutor(aggregatorName, duration,
                        processExpressionExecutorsMap.get(duration), shouldUpdateTimestamp,
                        groupByKeyGeneratorList.get(duration), isRoot, aggregationTables.get(duration),
                        child, siddhiQueryContext, processedMetaStreamEvent, timeZone);

                incrementalExecutorMap.put(duration, incrementalExecutor);
                root = incrementalExecutor;
            }
        }
        return incrementalExecutorMap;
    }

    private static List<ExpressionExecutor> constructProcessExpressionExecutors(
            SiddhiQueryContext siddhiQueryContext, Map<String, Table> tableMap, int baseAggregatorBeginIndex,
            List<Expression> finalBaseExpressions, StreamDefinition incomingOutputStreamDefinition,
            MetaStreamEvent processedMetaStreamEvent,
            List<VariableExpressionExecutor> processVariableExpressionExecutors, boolean isProcessingOnExternalTime,
            TimePeriod.Duration duration, boolean isDistributed, String shardId, boolean isLatestEventColAdded) {

        List<ExpressionExecutor> processExpressionExecutors = new ArrayList<>();
        List<Attribute> attributeList = incomingOutputStreamDefinition.getAttributeList();

        int i = 1;
        //Add timestamp executor
        Attribute attribute = attributeList.get(0);
        VariableExpressionExecutor variableExpressionExecutor = (VariableExpressionExecutor) ExpressionParser
                .parseExpression(new Variable(attribute.getName()), processedMetaStreamEvent, 0, tableMap,
                        processVariableExpressionExecutors, true, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
        processExpressionExecutors.add(variableExpressionExecutor);

        if (isDistributed) {
            Expression shardIdExpression = Expression.value(shardId);
            ExpressionExecutor shardIdExpressionExecutor = ExpressionParser.parseExpression(shardIdExpression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors, true, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(shardIdExpressionExecutor);
            i++;
        }

        if (isProcessingOnExternalTime) {
            Expression externalTimestampExpression =
                    AttributeFunction.function("incrementalAggregator", "getAggregationStartTime",
                            new Variable(AGG_EXTERNAL_TIMESTAMP_COL), new StringConstant(duration.name()));
            ExpressionExecutor externalTimestampExecutor = ExpressionParser.parseExpression(
                    externalTimestampExpression, processedMetaStreamEvent, 0, tableMap,
                    processVariableExpressionExecutors, true, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(externalTimestampExecutor);
            i++;
        }

        if (isLatestEventColAdded) {
            baseAggregatorBeginIndex = baseAggregatorBeginIndex - 1;
        }

        for (; i < baseAggregatorBeginIndex; i++) {
            attribute = attributeList.get(i);
            variableExpressionExecutor = (VariableExpressionExecutor) ExpressionParser.parseExpression(
                    new Variable(attribute.getName()), processedMetaStreamEvent, 0, tableMap,
                    processVariableExpressionExecutors, true, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(variableExpressionExecutor);
        }

        if (isLatestEventColAdded) {
            Expression lastTimestampExpression =
                    AttributeFunction.function("max", new Variable(AGG_LAST_TIMESTAMP_COL));
            ExpressionExecutor latestTimestampExecutor = ExpressionParser.parseExpression(lastTimestampExpression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors, true, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(latestTimestampExecutor);
        }

        for (Expression expression : finalBaseExpressions) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors,
                    true, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(expressionExecutor);
        }
        return processExpressionExecutors;
    }

    private static QuerySelector generateQuerySelector(SiddhiQueryContext siddhiQueryContext) {
        Selector selector = new Selector();
        List<Attribute> attributeList = generateCUDInputStreamAttributes();
        for (Attribute attribute : attributeList) {
            selector.select(new Variable(attribute.getName()));
        }
        return new QuerySelector("BarStream", selector, true, false,
                siddhiQueryContext);
    }

    private static boolean populateFinalBaseAggregators(
            Map<String, Table> tableMap, List<VariableExpressionExecutor> incomingVariableExpressionExecutors,
            MetaStreamEvent incomingMetaStreamEvent, List<ExpressionExecutor> incomingExpressionExecutors,
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators,
            SiddhiQueryContext siddhiQueryContext, List<Expression> finalBaseAggregators) {

        boolean isOptimisedLookup = true;

        List<Attribute> finalBaseAttributes = new ArrayList<>();

        for (IncrementalAttributeAggregator incrementalAttributeAggregator : incrementalAttributeAggregators) {
            Attribute[] baseAttributes = incrementalAttributeAggregator.getBaseAttributes();
            Expression[] baseAttributeInitialValues = incrementalAttributeAggregator.getBaseAttributeInitialValues();
            Expression[] baseAggregators = incrementalAttributeAggregator.getBaseAggregators();

            if (baseAggregators.length > 1) {
                isOptimisedLookup = false;
            }

            for (int i = 0; i < baseAttributes.length; i++) {
                validateBaseAggregators(incrementalAttributeAggregators,
                        incrementalAttributeAggregator, baseAttributes,
                        baseAttributeInitialValues, baseAggregators, i);
                if (!finalBaseAttributes.contains(baseAttributes[i])) {
                    finalBaseAttributes.add(baseAttributes[i]);
                    finalBaseAggregators.add(baseAggregators[i]);
                    incomingMetaStreamEvent.addOutputData(baseAttributes[i]);
                    incomingExpressionExecutors.add(ExpressionParser.parseExpression(baseAttributeInitialValues[i],
                            incomingMetaStreamEvent, 0, tableMap, incomingVariableExpressionExecutors,
                            false, 0,
                            ProcessingMode.BATCH, false, siddhiQueryContext));
                }
            }
        }
        return isOptimisedLookup;
    }

    private static void populateIncomingAggregatorsAndExecutors(
            AggregationDefinition aggregationDefinition, SiddhiQueryContext siddhiQueryContext,
            Map<String, Table> tableMap, List<VariableExpressionExecutor> incomingVariableExpressionExecutors,
            MetaStreamEvent incomingMetaStreamEvent, List<ExpressionExecutor> incomingExpressionExecutors,
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators, List<Variable> groupByVariableList,
            List<Expression> outputExpressions, boolean isProcessingOnExternalTime, boolean isDistributed,
            String shardId) {

        boolean isLatestEventAdded = false;

        ExpressionExecutor timestampExecutor = getTimeStampExecutor(siddhiQueryContext, tableMap,
                incomingVariableExpressionExecutors, incomingMetaStreamEvent);

        Attribute timestampAttribute = new Attribute(AGG_START_TIMESTAMP_COL, Attribute.Type.LONG);
        incomingMetaStreamEvent.addOutputData(timestampAttribute);
        incomingExpressionExecutors.add(timestampExecutor);

        if (isDistributed) {
            ExpressionExecutor nodeIdExpExecutor = new ConstantExpressionExecutor(shardId, Attribute.Type.STRING);
            incomingExpressionExecutors.add(nodeIdExpExecutor);
            incomingMetaStreamEvent.addOutputData(new Attribute(AGG_SHARD_ID_COL, Attribute.Type.STRING));
        }

        ExpressionExecutor externalTimestampExecutor = null;
        if (isProcessingOnExternalTime) {
            Expression externalTimestampExpression = aggregationDefinition.getAggregateAttribute();
            externalTimestampExecutor = ExpressionParser.parseExpression(externalTimestampExpression,
                    incomingMetaStreamEvent, 0, tableMap, incomingVariableExpressionExecutors,
                    false, 0, ProcessingMode.BATCH, false, siddhiQueryContext);

            if (externalTimestampExecutor.getReturnType() == Attribute.Type.STRING) {
                Expression expression = AttributeFunction.function("incrementalAggregator",
                        "timestampInMilliseconds", externalTimestampExpression);
                externalTimestampExecutor = ExpressionParser.parseExpression(expression, incomingMetaStreamEvent,
                        0, tableMap, incomingVariableExpressionExecutors, false, 0, ProcessingMode.BATCH, false,
                        siddhiQueryContext);
            } else if (externalTimestampExecutor.getReturnType() != Attribute.Type.LONG) {
                throw new SiddhiAppCreationException(
                        "Aggregation Definition '" + aggregationDefinition.getId() + "'s timestamp attribute expects " +
                                "long or string, but found " + externalTimestampExecutor.getReturnType() + ". Hence, can't " +
                                "create the siddhi app '" + siddhiQueryContext.getSiddhiAppContext().getName() + "'",
                        externalTimestampExpression.getQueryContextStartIndex(),
                        externalTimestampExpression.getQueryContextEndIndex());
            }

            Attribute externalTimestampAttribute = new Attribute(AGG_EXTERNAL_TIMESTAMP_COL, Attribute.Type.LONG);
            incomingMetaStreamEvent.addOutputData(externalTimestampAttribute);
            incomingExpressionExecutors.add(externalTimestampExecutor);
        }

        AbstractDefinition incomingLastInputStreamDefinition = incomingMetaStreamEvent.getLastInputDefinition();
        for (Variable groupByVariable : groupByVariableList) {

            incomingMetaStreamEvent.addOutputData(incomingLastInputStreamDefinition.getAttributeList()
                    .get(incomingLastInputStreamDefinition.getAttributePosition(
                            groupByVariable.getAttributeName())));
            incomingExpressionExecutors.add(ExpressionParser.parseExpression(groupByVariable,
                    incomingMetaStreamEvent, 0, tableMap, incomingVariableExpressionExecutors,
                    false, 0, ProcessingMode.BATCH,
                    false, siddhiQueryContext));
        }

        // Add AGG_TIMESTAMP to output as well
        aggregationDefinition.getAttributeList().add(timestampAttribute);

        //Executors of time is differentiated with modes
        //check and set whether the aggregation in happened on an external timestamp
        if (isProcessingOnExternalTime) {
            outputExpressions.add(Expression.variable(AGG_EXTERNAL_TIMESTAMP_COL));
        } else {
            outputExpressions.add(Expression.variable(AGG_START_TIMESTAMP_COL));
        }

        for (OutputAttribute outputAttribute : aggregationDefinition.getSelector().getSelectionList()) {
            Expression expression = outputAttribute.getExpression();
            // If the select contains the aggregation function expression type will be AttributeFunction
            if (expression instanceof AttributeFunction) {
                IncrementalAttributeAggregator incrementalAggregator = null;
                try {
                    incrementalAggregator = (IncrementalAttributeAggregator)
                            SiddhiClassLoader.loadExtensionImplementation(
                                    new AttributeFunction("incrementalAggregator",
                                            ((AttributeFunction) expression).getName(),
                                            ((AttributeFunction) expression).getParameters()),
                                    IncrementalAttributeAggregatorExtensionHolder.getInstance(
                                            siddhiQueryContext.getSiddhiAppContext()));
                } catch (SiddhiAppCreationException ex) {
                    try {
                        SiddhiClassLoader.loadExtensionImplementation((AttributeFunction) expression,
                                FunctionExecutorExtensionHolder.getInstance(siddhiQueryContext.getSiddhiAppContext()));
                        processAggregationSelectors(aggregationDefinition, siddhiQueryContext, tableMap,
                                incomingVariableExpressionExecutors, incomingMetaStreamEvent,
                                incomingExpressionExecutors, outputExpressions, outputAttribute, expression);

                    } catch (SiddhiAppCreationException e) {
                        throw new SiddhiAppCreationException("'" + ((AttributeFunction) expression).getName() +
                                "' is neither a incremental attribute aggregator extension or a function" +
                                " extension", expression.getQueryContextStartIndex(),
                                expression.getQueryContextEndIndex());
                    }
                }
                if (incrementalAggregator != null) {
                    initIncrementalAttributeAggregator(incomingLastInputStreamDefinition,
                            (AttributeFunction) expression, incrementalAggregator);
                    incrementalAttributeAggregators.add(incrementalAggregator);
                    aggregationDefinition.getAttributeList().add(
                            new Attribute(outputAttribute.getRename(), incrementalAggregator.getReturnType()));
                    outputExpressions.add(incrementalAggregator.aggregate());
                }
            } else {
                if (expression instanceof Variable && groupByVariableList.contains(expression)) {
                    Attribute groupByAttribute = null;
                    for (Attribute attribute : incomingMetaStreamEvent.getOutputData()) {
                        if (attribute.getName().equals(((Variable) expression).getAttributeName())) {
                            groupByAttribute = attribute;
                            break;
                        }
                    }
                    if (groupByAttribute == null) {
                        throw new SiddhiAppCreationException("Expected GroupBy attribute '" +
                                ((Variable) expression).getAttributeName() + "' not used in aggregation '" +
                                siddhiQueryContext.getName() + "' processing.", expression.getQueryContextStartIndex(),
                                expression.getQueryContextEndIndex());
                    }
                    aggregationDefinition.getAttributeList().add(
                            new Attribute(outputAttribute.getRename(), groupByAttribute.getType()));
                    outputExpressions.add(Expression.variable(groupByAttribute.getName()));

                } else {
                    isLatestEventAdded = true;
                    processAggregationSelectors(aggregationDefinition, siddhiQueryContext, tableMap,
                            incomingVariableExpressionExecutors, incomingMetaStreamEvent,
                            incomingExpressionExecutors, outputExpressions, outputAttribute, expression);
                }

            }
        }

        if (isProcessingOnExternalTime && isLatestEventAdded) {
            Attribute lastEventTimeStamp = new Attribute(AGG_LAST_TIMESTAMP_COL, Attribute.Type.LONG);
            incomingMetaStreamEvent.addOutputData(lastEventTimeStamp);
            incomingExpressionExecutors.add(externalTimestampExecutor);
        }

    }

    private static void processAggregationSelectors(AggregationDefinition aggregationDefinition,
                                                    SiddhiQueryContext siddhiQueryContext, Map<String, Table> tableMap,
                                                    List<VariableExpressionExecutor> incomingVariableExpressionExecutors,
                                                    MetaStreamEvent incomingMetaStreamEvent,
                                                    List<ExpressionExecutor> incomingExpressionExecutors,
                                                    List<Expression> outputExpressions, OutputAttribute outputAttribute,
                                                    Expression expression) {

        ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression, incomingMetaStreamEvent,
                0, tableMap, incomingVariableExpressionExecutors, false, 0, ProcessingMode.BATCH, false,
                siddhiQueryContext);
        incomingExpressionExecutors.add(expressionExecutor);
        incomingMetaStreamEvent.addOutputData(
                new Attribute(outputAttribute.getRename(), expressionExecutor.getReturnType()));
        aggregationDefinition.getAttributeList().add(
                new Attribute(outputAttribute.getRename(), expressionExecutor.getReturnType()));
        outputExpressions.add(Expression.variable(outputAttribute.getRename()));
    }


    private static void validateBaseAggregators(List<IncrementalAttributeAggregator> incrementalAttributeAggregators,
                                                IncrementalAttributeAggregator incrementalAttributeAggregator,
                                                Attribute[] baseAttributes, Expression[] baseAttributeInitialValues,
                                                Expression[] baseAggregators, int i) {
        for (int i1 = i; i1 < incrementalAttributeAggregators.size(); i1++) {
            IncrementalAttributeAggregator otherAttributeAggregator = incrementalAttributeAggregators.get(i1);
            if (otherAttributeAggregator != incrementalAttributeAggregator) {
                Attribute[] otherBaseAttributes = otherAttributeAggregator.getBaseAttributes();
                Expression[] otherBaseAttributeInitialValues = otherAttributeAggregator
                        .getBaseAttributeInitialValues();
                Expression[] otherBaseAggregators = otherAttributeAggregator.getBaseAggregators();
                for (int j = 0; j < otherBaseAttributes.length; j++) {
                    if (baseAttributes[i].equals(otherBaseAttributes[j])) {
                        if (!baseAttributeInitialValues[i].equals(otherBaseAttributeInitialValues[j])) {
                            throw new SiddhiAppCreationException("BaseAttributes having same name should " +
                                    "be defined with same initial values, but baseAttribute '" +
                                    baseAttributes[i] + "' is defined in '" +
                                    incrementalAttributeAggregator.getClass().getName() + "' and '" +
                                    otherAttributeAggregator.getClass().getName() +
                                    "' with different initial values.");
                        }
                        if (!baseAggregators[i].equals(otherBaseAggregators[j])) {
                            throw new SiddhiAppCreationException("BaseAttributes having same name should " +
                                    "be defined with same baseAggregators, but baseAttribute '" +
                                    baseAttributes[i] + "' is defined in '" +
                                    incrementalAttributeAggregator.getClass().getName() + "' and '" +
                                    otherAttributeAggregator.getClass().getName() +
                                    "' with different baseAggregators.");
                        }
                    }
                }
            }
        }
    }

    private static void initIncrementalAttributeAggregator(AbstractDefinition lastInputStreamDefinition,
                                                           AttributeFunction attributeFunction,
                                                           IncrementalAttributeAggregator incrementalAttributeAggregator) {

        String attributeName = null;
        Attribute.Type attributeType = null;
        if (attributeFunction.getParameters() != null && attributeFunction.getParameters()[0] != null) {
            if (attributeFunction.getParameters().length != 1) {
                throw new SiddhiAppCreationException("Incremental aggregator requires only one parameter. "
                        + "Found " + attributeFunction.getParameters().length,
                        attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
            }
            if (!(attributeFunction.getParameters()[0] instanceof Variable)) {
                throw new SiddhiAppCreationException("Incremental aggregator expected a variable. " +
                        "However a parameter of type " + attributeFunction.getParameters()[0].getClass().getTypeName()
                        + " was found",
                        attributeFunction.getParameters()[0].getQueryContextStartIndex(),
                        attributeFunction.getParameters()[0].getQueryContextEndIndex());
            }
            attributeName = ((Variable) attributeFunction.getParameters()[0]).getAttributeName();
            attributeType = lastInputStreamDefinition.getAttributeType(attributeName);
        }
        //This will initialize the aggregation function(avg, count, sum, etc) and figure out the parameters that needed
        //to store in table eg: count() => AGG_COUNT
        incrementalAttributeAggregator.init(attributeName, attributeType);

        Attribute[] baseAttributes = incrementalAttributeAggregator.getBaseAttributes();
        Expression[] baseAttributeInitialValues = incrementalAttributeAggregator
                .getBaseAttributeInitialValues();
        Expression[] baseAggregators = incrementalAttributeAggregator.getBaseAggregators();

        if (baseAttributes.length != baseAggregators.length) {
            throw new SiddhiAppCreationException("Number of baseAggregators '" +
                    baseAggregators.length + "' and baseAttributes '" +
                    baseAttributes.length + "' is not equal for '" + attributeFunction + "'",
                    attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
        }
        if (baseAttributeInitialValues.length != baseAggregators.length) {
            throw new SiddhiAppCreationException("Number of baseAggregators '" +
                    baseAggregators.length + "' and baseAttributeInitialValues '" +
                    baseAttributeInitialValues.length + "' is not equal for '" +
                    attributeFunction + "'",
                    attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
        }
    }

    private static ExpressionExecutor getTimeStampExecutor(SiddhiQueryContext siddhiQueryContext,
                                                           Map<String, Table> tableMap,
                                                           List<VariableExpressionExecutor> variableExpressionExecutors,
                                                           MetaStreamEvent metaStreamEvent) {

        Expression timestampExpression;
        ExpressionExecutor timestampExecutor;

        // Execution is based on system time, the GMT time zone would be used.
        timestampExpression = AttributeFunction.function("currentTimeMillis", null);
        timestampExecutor = ExpressionParser.parseExpression(timestampExpression, metaStreamEvent, 0, tableMap,
                variableExpressionExecutors, false, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
        return timestampExecutor;
    }

    private static boolean isRange(TimePeriod timePeriod) {
        return timePeriod.getOperator() == TimePeriod.Operator.RANGE;
    }

    private static List<TimePeriod.Duration> getSortedPeriods(TimePeriod timePeriod) {
        try {
            List<TimePeriod.Duration> durations = timePeriod.getDurations();
            if (isRange(timePeriod)) {
                durations = fillGap(durations.get(0), durations.get(1));
            }
            return sortedDurations(durations);
        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, timePeriod, null);
            throw t;
        }
    }

    private static List<TimePeriod.Duration> sortedDurations(List<TimePeriod.Duration> durations) {
        List<TimePeriod.Duration> copyDurations = new ArrayList<>(durations);

        Comparator periodComparator = (Comparator<TimePeriod.Duration>) (firstDuration, secondDuration) -> {
            int firstOrdinal = firstDuration.ordinal();
            int secondOrdinal = secondDuration.ordinal();
            if (firstOrdinal > secondOrdinal) {
                return 1;
            } else if (firstOrdinal < secondOrdinal) {
                return -1;
            }
            return 0;
        };
        copyDurations.sort(periodComparator);
        return copyDurations;
    }

    private static List<TimePeriod.Duration> fillGap(TimePeriod.Duration start, TimePeriod.Duration end) {
        TimePeriod.Duration[] durations = TimePeriod.Duration.values();
        List<TimePeriod.Duration> filledDurations = new ArrayList<>();

        int startIndex = start.ordinal();
        int endIndex = end.ordinal();

        if (startIndex > endIndex) {
            throw new SiddhiAppCreationException(
                    "Start time period must be less than end time period for range aggregation calculation");
        }

        if (startIndex == endIndex) {
            filledDurations.add(start);
        } else {
            TimePeriod.Duration[] temp = new TimePeriod.Duration[endIndex - startIndex + 1];
            System.arraycopy(durations, startIndex, temp, 0, endIndex - startIndex + 1);
            filledDurations = Arrays.asList(temp);
        }
        return filledDurations;
    }

    private static HashMap<TimePeriod.Duration, Table> initDefaultTables(
            String aggregatorName, List<TimePeriod.Duration> durations,
            StreamDefinition streamDefinition, SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
            List<Annotation> annotations, List<Variable> groupByVariableList, boolean isProcessingOnExternalTime,
            boolean enablePartioning, List<TimePeriod.Duration> activeIncrementalDurations) {

        HashMap<TimePeriod.Duration, Table> aggregationTableMap = new HashMap<>();

        // Create annotations for primary key
        Annotation primaryKeyAnnotation = new Annotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY);
        primaryKeyAnnotation.element(null, AGG_START_TIMESTAMP_COL);

        if (enablePartioning) {
            primaryKeyAnnotation.element(null, AGG_SHARD_ID_COL);
        }
        if (isProcessingOnExternalTime) {
            primaryKeyAnnotation.element(null, AGG_EXTERNAL_TIMESTAMP_COL);
        }
        for (Variable groupByVariable : groupByVariableList) {
            primaryKeyAnnotation.element(null, groupByVariable.getAttributeName());
        }
        annotations.add(primaryKeyAnnotation);
        for (TimePeriod.Duration duration : activeIncrementalDurations) {
            String tableId = aggregatorName + "_" + duration.toString();
            TableDefinition tableDefinition = TableDefinition.id(tableId);
            for (Attribute attribute : streamDefinition.getAttributeList()) {
                tableDefinition.attribute(attribute.getName(), attribute.getType());
            }
            annotations.forEach(tableDefinition::annotation);
            siddhiAppRuntimeBuilder.defineTable(tableDefinition);
            aggregationTableMap.put(duration, siddhiAppRuntimeBuilder.getTableMap().get(tableId));
        }
        return aggregationTableMap;
    }

    private static Map<TimePeriod.Duration, Processor> initAggregateQueryExecutor(
            List<TimePeriod.Duration> aggregationDurations,
            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMap,
            StreamDefinition incomingOutputStreamDefinition, boolean isDistributed,
            String shardID, boolean isProcessingOnExternalTime, SiddhiQueryContext siddhiQueryContext,
            AggregationDefinition aggregationDefinition, ConfigManager configManager,
            Map<TimePeriod.Duration, Table> aggregationTables,
            List<TimePeriod.Duration> activeIncrementalDurations) {

        Map<TimePeriod.Duration, Processor> cudProcessors = new LinkedHashMap<>();
        String datasourceName = AnnotationHelper.getAnnotationElement(SiddhiConstants.NAMESPACE_STORE,
                "datasource", aggregationDefinition.getAnnotations()).getValue();
        Database databaseType = getDatabaseType(configManager, aggregationDefinition, datasourceName);

        SiddhiAppContext cudSiddhiAppContext = new SiddhiAppContext();
        SiddhiContext context = new SiddhiContext();
        context.setConfigManager(configManager);
        cudSiddhiAppContext.setSiddhiContext(context);
        StringConstant datasource = new StringConstant(datasourceName);
        ConstantExpressionExecutor datasourceExecutor = new ConstantExpressionExecutor(datasource.getValue(),
                Attribute.Type.STRING);
        List<Expression> queryParams = new LinkedList<>();
        try {
            DBAggregationQueryConfigurationEntry dbAggregationQueryConfigurationEntry = DBAggregationQueryUtil.
                    lookupCurrentQueryConfigurationEntry(databaseType);

            for (int i = activeIncrementalDurations.size() - 1; i > 0; i--) {
                if (activeIncrementalDurations.contains(aggregationDurations.get(i))) {
                    if (aggregationDurations.get(i) != TimePeriod.Duration.SECONDS ||
                            aggregationDurations.get(i) != TimePeriod.Duration.MINUTES ||
                            aggregationDurations.get(i) != TimePeriod.Duration.HOURS) {
                        log.debug(" Initializing cudProcessors for durations ");
                        List<ExpressionExecutor> cudStreamProcessorInputVariables = new ArrayList<>();
                        String databaseSelectQuery = generateDatabaseQuery(processExpressionExecutorsMap.
                                        get(aggregationDurations.get(i)), dbAggregationQueryConfigurationEntry,
                                incomingOutputStreamDefinition, isDistributed, shardID, isProcessingOnExternalTime,
                                aggregationTables.get(aggregationDurations.get(i)),
                                aggregationTables.get(aggregationDurations.get(i - 1)));
                        StringConstant selectQuery = new StringConstant(databaseSelectQuery);
                        ConstantExpressionExecutor selectExecutor = new ConstantExpressionExecutor(selectQuery.getValue(),
                                Attribute.Type.STRING);
                        List<Attribute> cudInputStreamAttributesList = generateCUDInputStreamAttributes();
                        cudStreamProcessorInputVariables.add(datasourceExecutor);
                        cudStreamProcessorInputVariables.add(selectExecutor);
                        queryParams.add(datasource);
                        queryParams.add(selectQuery);
                        MetaStreamEvent metaStreamEvent = generateCUDMetaStreamEvent();
                        StreamDefinition outputStream = new StreamDefinition();
                        VariableExpressionExecutor variable;
                        int j = 0;
                        for (Attribute attribute : cudInputStreamAttributesList) {
                            queryParams.add(new Variable(attribute.getName()));
                            variable = new VariableExpressionExecutor(attribute, 0, 0);
                            variable.setPosition(new int[]{2, j});
                            cudStreamProcessorInputVariables.add(variable);
                            if (attribute.getName().equals(UPDATED_TIMESTAMP)) {
                                queryParams.add(new Variable(attribute.getName()));
                                variable = new VariableExpressionExecutor(attribute, 0, 0);
                                variable.setPosition(new int[]{2, j});
                                cudStreamProcessorInputVariables.add(variable);
                            }
                            outputStream.attribute(attribute.getName(), attribute.getType());
                            j++;
                        }

                        Expression[] streamHandler = new Expression[queryParams.size()];
                        int l = 0;
                        for (Expression expression : queryParams) {
                            streamHandler[i] = expression;
                            l++;
                        }
                        StreamFunction cudStreamFunction = new StreamFunction("rdbms", "cud", streamHandler);

                        cudProcessors.put(aggregationDurations.get(i), getCudProcessor(cudStreamFunction, siddhiQueryContext,
                                metaStreamEvent, cudStreamProcessorInputVariables, false, aggregationDurations.get(i)));
                    }
                } else {
                    cudProcessors.put(aggregationDurations.get(i), null);
                }
            }
            return cudProcessors;
        } catch (CannotLoadConfigurationException e) {
            throw new SiddhiAppCreationException("Error occurred while initializing the persisted incremental " +
                    "aggregation. Could not load the db quires for database type " + databaseType);
        }
    }

    private static MetaStreamEvent generateCUDMetaStreamEvent() {
        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        List<Attribute> cudInputStreamAttributesList = generateCUDInputStreamAttributes();
        StreamDefinition inputDefinition = new StreamDefinition();
        for (Attribute attribute : cudInputStreamAttributesList) {
            metaStreamEvent.addData(attribute);
            inputDefinition.attribute(attribute.getName(), attribute.getType());
        }
        metaStreamEvent.addInputDefinition(inputDefinition);
        metaStreamEvent.setEventType(MetaStreamEvent.EventType.DEFAULT);
        return metaStreamEvent;
    }

    private static String generateDatabaseQuery(List<ExpressionExecutor> expressionExecutors,
                                                DBAggregationQueryConfigurationEntry
                                                        dbAggregationQueryConfigurationEntry,
                                                StreamDefinition incomingOutputStreamDefinition,
                                                boolean isDistributed, String shardID,
                                                boolean isProcessingOnExternalTime, Table aggregationTable,
                                                Table parentAggregationTable) {

        DBAggregationSelectFunctionTemplate dbAggregationSelectFunctionTemplates = dbAggregationQueryConfigurationEntry.
                getRdbmsSelectFunctionTemplate();
        DBAggregationSelectQueryTemplate dbAggregationSelectQueryTemplate = dbAggregationQueryConfigurationEntry.
                getRdbmsSelectQueryTemplate();
        List<Attribute> attributeList = incomingOutputStreamDefinition.getAttributeList();
        StringJoiner selectColumnJoiner = new StringJoiner(", ");
        String selectQueryClause;
        StringJoiner groupByQueryBuilder = new StringJoiner(", ");
        StringBuilder filterQueryBuilder = new StringBuilder();
        StringBuilder insertIntoQueryBuilder = new StringBuilder();
        StringJoiner finalQuery = new StringJoiner(" ");

        insertIntoQueryBuilder.append(dbAggregationSelectQueryTemplate.getRecordInsertQuery().replace(PLACEHOLDER_TABLE_NAME,
                aggregationTable.getTableDefinition().getId()));
        int i = 0;
        for (ExpressionExecutor expressionExecutor : expressionExecutors) {
            if (expressionExecutor instanceof VariableExpressionExecutor) {
                VariableExpressionExecutor variableExpressionExecutor = (VariableExpressionExecutor) expressionExecutor;
                if (variableExpressionExecutor.getAttribute().getName()
                        .equals(AGG_START_TIMESTAMP_COL) ||
                        variableExpressionExecutor.getAttribute().getName().equals(AGG_EXTERNAL_TIMESTAMP_COL)) {
                    selectColumnJoiner.add(" ? ");
                } else {
                    selectColumnJoiner.add(variableExpressionExecutor.getAttribute().getName());
                    groupByQueryBuilder.add(variableExpressionExecutor.getAttribute().getName());
                }
            } else if (expressionExecutor instanceof IncrementalAggregateBaseTimeFunctionExecutor) {
                selectColumnJoiner.add(" ? ");
            } else if (expressionExecutor instanceof MaxAttributeAggregatorExecutor) {
                selectColumnJoiner.add(dbAggregationSelectFunctionTemplates.getMaxFunction().
                        replace(PLACEHOLDER_COLUMN, attributeList.get(i).getName()));
            } else if (expressionExecutor instanceof ConstantExpressionExecutor) {
                selectColumnJoiner.add(attributeList.get(i).getName());
                groupByQueryBuilder.add(attributeList.get(i).getName());
            } else if (expressionExecutor instanceof SumAttributeAggregatorExecutor) {
                selectColumnJoiner.add(dbAggregationSelectFunctionTemplates.getSumFunction().
                        replace(PLACEHOLDER_COLUMN, attributeList.get(i).getName()));
            }
            i++;
        }

        selectQueryClause = dbAggregationSelectQueryTemplate.getSelectClause().replace(PLACEHOLDER_SELECTORS,
                selectColumnJoiner.toString()).replace(PLACEHOLDER_TABLE_NAME,
                parentAggregationTable.getTableDefinition().getId());

        if (isProcessingOnExternalTime) {
            filterQueryBuilder.append(" (").append(AGG_EXTERNAL_TIMESTAMP_COL).append(" >= ?").append(" AND ")
                    .append(AGG_EXTERNAL_TIMESTAMP_COL).append(" <= ? ").append(") ");
        } else {
            filterQueryBuilder.append(" (").append(AGG_START_TIMESTAMP_COL).append(" >= ?").append(" AND ")
                    .append(AGG_START_TIMESTAMP_COL).append(" <= ? ").append(") ");
        }
        if (isDistributed) {
            filterQueryBuilder.append(" AND ").append(AGG_SHARD_ID_COL).append(" = '").append(shardID).append("' ");
        }
        finalQuery.add(insertIntoQueryBuilder).add(selectQueryClause).add(dbAggregationSelectQueryTemplate.getWhereClause()
                .replace(PLACEHOLDER_CONDITION, filterQueryBuilder)).add(dbAggregationSelectQueryTemplate.getGroupByClause()
                .replace(PLACEHOLDER_COLUMNS, groupByQueryBuilder.toString()));
        return finalQuery.toString();

    }

    private static List<Attribute> generateCUDInputStreamAttributes() {
        List<Attribute> cudInputStreamAttributeList = new ArrayList<>();
        cudInputStreamAttributeList.add(new Attribute(UPDATED_TIMESTAMP, Attribute.Type.LONG));
        cudInputStreamAttributeList.add(new Attribute(FROM_TIMESTAMP, Attribute.Type.LONG));
        cudInputStreamAttributeList.add(new Attribute(TO_TIMESTAMP, Attribute.Type.LONG));

        return cudInputStreamAttributeList;
    }

    private static Database getDatabaseType(ConfigManager configManager,
                                            AggregationDefinition aggregationDefinition, String datasourceName) {
        ConfigReader configReader = configManager.generateConfigReader("wso2.datasources", datasourceName);
        String databaseType = configReader.readConfig("driverClassName", "null").toLowerCase();
        if (databaseType.contains("mysql")) {
            return Database.MYSQL;
        } else if (databaseType.contains("oracle")) {
            return Database.ORACLE;
        } else if (databaseType.contains("mssql")) {
            return Database.MSSQL;
        } else if (databaseType.contains("db2")) {
            return Database.DB2;
        } else if (databaseType.contains("h2")) {
            return Database.H2;
        } else {
            log.warn("Provided database type " + databaseType + "is not recognized as a supported database type for" +
                    " persisted incremental aggregation, using MySQL as default ");
            return Database.MYSQL;
        }
    }

    public static StreamEvent createRestEvent(MetaStreamEvent metaStreamEvent, StreamEvent streamEvent) {
        streamEvent.setTimestamp(0);
        streamEvent.setType(ComplexEvent.Type.RESET);
        List<Attribute> outputData = metaStreamEvent.getOutputData();
        for (int i = 0, outputDataSize = outputData.size(); i < outputDataSize; i++) {
            Attribute attribute = outputData.get(i);
            switch (attribute.getType()) {

                case STRING:
                    streamEvent.setOutputData("", i);
                    break;
                case INT:
                    streamEvent.setOutputData(0, i);
                    break;
                case LONG:
                    streamEvent.setOutputData(0L, i);
                    break;
                case FLOAT:
                    streamEvent.setOutputData(0f, i);
                    break;
                case DOUBLE:
                    streamEvent.setOutputData(0.0, i);
                    break;
                case BOOL:
                    streamEvent.setOutputData(false, i);
                    break;
                case OBJECT:
                    streamEvent.setOutputData(null, i);
                    break;
            }
        }
        return streamEvent;
    }


    private static Processor getCudProcessor(StreamHandler streamHandler, SiddhiQueryContext siddhiQueryContext,
                                             MetaStreamEvent metaStreamEvent,
                                             List<ExpressionExecutor> attributeExpressionExecutors,
                                             boolean outputExpectsExpiredEvents, TimePeriod.Duration duration) {
        ConfigReader configReader = siddhiQueryContext.getSiddhiContext().getConfigManager().
                generateConfigReader(((StreamFunction) streamHandler).getNamespace(),
                        ((StreamFunction) streamHandler).getName());
        ExpressionExecutor[] expressionExecutors = new ExpressionExecutor[attributeExpressionExecutors.size()];
        int i = 0;
        MetaStreamEvent customMetaStreamEvent = metaStreamEvent;
        for (ExpressionExecutor variableExpressionExecutors : attributeExpressionExecutors) {
            expressionExecutors[i] = variableExpressionExecutors;
            i++;
        }
        AbstractStreamProcessor abstractStreamProcessor;
        abstractStreamProcessor = (StreamProcessor) SiddhiClassLoader.loadExtensionImplementation(
                (Extension) streamHandler,
                StreamProcessorExtensionHolder.getInstance(siddhiQueryContext.getSiddhiAppContext()));
        abstractStreamProcessor.initProcessor(customMetaStreamEvent, expressionExecutors
                , configReader, outputExpectsExpiredEvents, false, false, streamHandler, siddhiQueryContext);
        if (customMetaStreamEvent.getInputDefinitions().size() == 2) {
            AbstractDefinition outputDefinition = customMetaStreamEvent.getInputDefinitions().get(1);
            List<Attribute> outputAttributes = outputDefinition.getAttributeList();
            for (Attribute attribute : outputAttributes) {
                customMetaStreamEvent.addOutputData(attribute);
            }
            customMetaStreamEvent.setOutputDefinition((StreamDefinition) outputDefinition);
        }
        abstractStreamProcessor.constructStreamEventPopulater(customMetaStreamEvent, 0);
        abstractStreamProcessor.setNextProcessor(new CUDDataProcessor(duration));
        return abstractStreamProcessor;
    }

    public static Map<String, Map<TimePeriod.Duration, Executor>> getAggregationDurationExecutorMap() {
        return aggregationDurationExecutorMap;
    }

    public enum Database {
        MYSQL,
        ORACLE,
        MSSQL,
        DB2,
        H2,
        DEFAULT
    }
}
