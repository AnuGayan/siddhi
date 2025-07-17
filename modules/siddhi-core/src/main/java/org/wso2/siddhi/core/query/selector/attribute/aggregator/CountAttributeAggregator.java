/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient;
import org.wso2.siddhi.core.util.kvstore.KVStoreManager; // Changed
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreException;
import org.wso2.siddhi.query.api.definition.Attribute;
// import redis.clients.jedis.Jedis; // Removed
// import redis.clients.jedis.exceptions.JedisException; // Removed
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.AbstractMap;
import java.util.Map;

public class CountAttributeAggregator extends AttributeAggregator {

    private static final Logger log = LoggerFactory.getLogger(CountAttributeAggregator.class);
    private static Attribute.Type type = Attribute.Type.LONG;
    private long value = 0L; // Local fallback counter
    private KeyValueStoreClient kvStoreClient;
    // private String kvStoreType; // Removed
    private String kvStoreKey;
    private String elementId;


    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.elementId = executionPlanContext.getElementIdGenerator().createNewId();
        this.kvStoreKey = "siddhi:count:" + executionPlanContext.getName() + ":" + elementId;

        try {
            this.kvStoreClient = KVStoreManager.getClient();
            if (this.kvStoreClient != null && this.kvStoreClient.isConnected()) {
                log.info("KVStoreClient initialized and connected for aggregator with key '{}'.", kvStoreKey);
            } else {
                log.warn("KVStoreClient obtained, but isConnected() is false for key '{}'. Aggregator will use fallback.",
                        kvStoreKey);
                this.kvStoreClient = null; // Ensure fallback
            }
        } catch (KeyValueStoreException e) {
            log.warn("Failed to get KVStoreClient for aggregator with key '{}'. Reason: {}. Operating in fallback mode.",
                    kvStoreKey, e.getMessage());
            this.kvStoreClient = null; // Ensure fallback
        } catch (Exception e) { // Catch any other unexpected exceptions during client init
            log.error("Unexpected error getting KVStoreClient for aggregator with key '{}'. Operating in fallback mode.",
                    kvStoreKey, e);
            this.kvStoreClient = null; // Ensure fallback
        }
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data) {
        if (kvStoreClient != null && kvStoreClient.isConnected()) {
            try {
                long newValue = kvStoreClient.increment(kvStoreKey);
                value = newValue; // Sync local fallback
                return newValue;
            } catch (KeyValueStoreException e) {
                log.error("Error incrementing count in Key-Value store for key '{}'. Falling back to local counter. Error: {}",
                        kvStoreKey, e.getMessage());
                // Potentially mark client as disconnected for a while or just fallback for this op
            }
        } else {
            if (kvStoreClient == null) {
                log.trace("processAdd: KeyValueStoreClient is null for key '{}'. Using local counter.", kvStoreKey);
            } else { // Implies !kvStoreClient.isConnected()
                log.warn("processAdd: KeyValueStoreClient not connected for key '{}'. Using local counter.", kvStoreKey);
            }
        }
        // Fallback to local counter
        value++;
        return value;
    }

    @Override
    public Object processAdd(Object[] data) {
        // This method is called when processing a batch of events.
        // For simplicity, we'll just call the single-event method.
        // A more optimized approach might involve Redis pipelines.
        return processAdd((Object) data);
    }

    @Override
    public Object processRemove(Object data) {
        if (kvStoreClient != null && kvStoreClient.isConnected()) {
            try {
                long newValue = kvStoreClient.decrement(kvStoreKey);
                value = newValue; // Sync local fallback
                return newValue;
            } catch (KeyValueStoreException e) {
                log.error("Error decrementing count in Key-Value store for key '{}'. Falling back to local counter. Error: {}",
                        kvStoreKey, e.getMessage());
            }
        } else {
            if (kvStoreClient == null) {
                log.trace("processRemove: KeyValueStoreClient is null for key '{}'. Using local counter.", kvStoreKey);
            } else {
                log.warn("processRemove: KeyValueStoreClient not connected for key '{}'. Using local counter.", kvStoreKey);
            }
        }
        // Fallback to local counter
        value--;
        return value;
    }

    @Override
    public Object processRemove(Object[] data) {
        // Similar to processAdd(Object[] data)
        return processRemove((Object) data);
    }

    @Override
    public Object reset() {
        if (kvStoreClient != null && kvStoreClient.isConnected()) {
            try {
                kvStoreClient.set(kvStoreKey, "0"); // Set to 0 for consistency
                value = 0L; // Sync local fallback
                return 0L;
            } catch (KeyValueStoreException e) {
                log.error("Error resetting count in Key-Value store for key '{}'. Falling back to local counter. Error: {}",
                        kvStoreKey, e.getMessage());
            }
        } else {
            if (kvStoreClient == null) {
                log.trace("reset: KeyValueStoreClient is null for key '{}'. Using local counter.", kvStoreKey);
            } else {
                log.warn("reset: KeyValueStoreClient not connected for key '{}'. Using local counter.", kvStoreKey);
            }
        }
        // Fallback to local counter
        value = 0L;
        return value;
    }

    @Override
    public void start() {
        // Nothing to start beyond what's in init
    }

    @Override
    public void stop() {
        // The lifecycle of the shared KVStoreClient is managed by KVStoreManager.
        // This aggregator instance should not disconnect or shut down the shared client.
        // The reference can be released if desired, but it's not strictly necessary.
        this.kvStoreClient = null;
        log.debug("Stopped aggregator for key '{}'. Released reference to shared KVStoreClient.", kvStoreKey);
    }

    @Override
    public Object[] currentState() {
        if (kvStoreClient != null && kvStoreClient.isConnected()) {
            try {
                String kvValueStr = kvStoreClient.get(kvStoreKey);
                if (kvValueStr != null) {
                    long kvValue = Long.parseLong(kvValueStr);
                    value = kvValue; // Sync local fallback
                    return new Object[]{new AbstractMap.SimpleEntry<>("Value", kvValue)};
                } else {
                    // Key doesn't exist in KV store. For a counter, this implies a value of 0.
                    // Sync local state to this understanding.
                    log.debug("Key '{}' not found in Key-Value store for currentState. Assuming 0.", kvStoreKey);
                    value = 0L;
                }
            } catch (NumberFormatException e) {
                log.error("Value for key '{}' in Key-Value store is not a valid Long. Falling back to local counter. Error: {}",
                        kvStoreKey, e.getMessage());
                // Local 'value' will be used below
            } catch (KeyValueStoreException e) {
                log.error("Error reading current state from Key-Value store for key '{}'. Falling back to local counter. Error: {}",
                        kvStoreKey, e.getMessage());
                // Local 'value' will be used below
            }
        } else {
            if (kvStoreClient == null) {
                log.trace("currentState: KeyValueStoreClient is null for key '{}'. Using local counter.", kvStoreKey);
            } else {
                log.warn("currentState: KeyValueStoreClient not connected for key '{}'. Using local counter.", kvStoreKey);
            }
        }
        // Fallback to local counter
        return new Object[]{new AbstractMap.SimpleEntry<>("Value", value)};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        long restoredValue = (Long) stateEntry.getValue();
        value = restoredValue; // Always update local value first, it's the ultimate fallback.

        if (kvStoreClient != null && kvStoreClient.isConnected()) {
            try {
                kvStoreClient.set(kvStoreKey, String.valueOf(restoredValue));
                log.info("Successfully restored state for key '{}' to {} in Key-Value store.", kvStoreKey, restoredValue);
            } catch (KeyValueStoreException e) {
                log.error("Error restoring state to Key-Value store for key '{}'. State only restored to local counter. Error: {}",
                        kvStoreKey, e.getMessage());
            }
        } else {
            if (kvStoreClient == null) {
                log.warn("restoreState: KeyValueStoreClient is null for key '{}'. State restored to local counter only.", kvStoreKey);
            } else {
                log.warn("restoreState: KeyValueStoreClient not connected for key '{}'. State restored to local counter only.", kvStoreKey);
            }
        }
    }
}
