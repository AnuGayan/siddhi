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
import org.wso2.siddhi.core.util.persistence.RedisConnectionManager; // Added
import org.wso2.siddhi.query.api.definition.Attribute;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.AbstractMap;
import java.util.Map;

public class CountAttributeAggregator extends AttributeAggregator {

    private static final Logger log = LoggerFactory.getLogger(CountAttributeAggregator.class);
    private static Attribute.Type type = Attribute.Type.LONG;
    private long value = 0L; // Local fallback counter
    // private Jedis jedis; // Removed: Will use RedisConnectionManager
    private String redisKey;
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
        this.redisKey = "siddhi:count:" + executionPlanContext.getName() + ":" + elementId;
        // Jedis connection is now managed by RedisConnectionManager, no direct connect here.
        // We can try to get a connection here to see if pool is usable, but not mandatory.
        // For instance, to log if Redis is available at init time:
        Jedis tempJedis = RedisConnectionManager.getJedis();
        if (tempJedis == null) {
            log.warn("CountAttributeAggregator (key: {}): Failed to get Jedis connection from pool during init. May operate in fallback mode.", redisKey);
        } else {
            log.info("CountAttributeAggregator (key: {}): Successfully initialized and can connect to Redis.", redisKey);
            RedisConnectionManager.closeJedis(tempJedis);
        }
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data) {
        Jedis jedis = RedisConnectionManager.getJedis();
        if (jedis != null) {
            try {
                Long redisValue = jedis.incr(redisKey);
                // Update local value as well to keep it in sync for potential future fallback
                value = redisValue;
                return redisValue;
            } catch (JedisException e) {
                log.error("Redis error in processAdd for key {}. Falling back to local counter.", redisKey, e);
                // Fallback logic is handled after finally
            } finally {
                RedisConnectionManager.closeJedis(jedis);
            }
        } else {
            log.warn("Could not get Jedis connection for processAdd (key {}). Using local counter.", redisKey);
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
        Jedis jedis = RedisConnectionManager.getJedis();
        if (jedis != null) {
            try {
                Long redisValue = jedis.decr(redisKey);
                // Update local value
                value = redisValue;
                return redisValue;
            } catch (JedisException e) {
                log.error("Redis error in processRemove for key {}. Falling back to local counter.", redisKey, e);
            } finally {
                RedisConnectionManager.closeJedis(jedis);
            }
        } else {
            log.warn("Could not get Jedis connection for processRemove (key {}). Using local counter.", redisKey);
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
        Jedis jedis = RedisConnectionManager.getJedis();
        if (jedis != null) {
            try {
                jedis.set(redisKey, "0");
                value = 0L; // Update local value
                return 0L;
            } catch (JedisException e) {
                log.error("Redis error in reset for key {}. Falling back to local counter.", redisKey, e);
            } finally {
                RedisConnectionManager.closeJedis(jedis);
            }
        } else {
            log.warn("Could not get Jedis connection for reset (key {}). Using local counter.", redisKey);
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
        // No longer need to close jedis connection here, managed by pool.
        // If RedisConnectionManager had a per-aggregator cleanup hook, it could go here.
    }

    @Override
    public Object[] currentState() {
        Jedis jedis = RedisConnectionManager.getJedis();
        if (jedis != null) {
            try {
                String redisValueStr = jedis.get(redisKey);
                if (redisValueStr != null) {
                    long redisValue = Long.parseLong(redisValueStr);
                    value = redisValue; // Sync local fallback
                    return new Object[]{new AbstractMap.SimpleEntry<String, Object>("Value", redisValue)};
                } else {
                    // Key doesn't exist in Redis, could be initial state or after a delete
                    // Use local value, and potentially set it in Redis if we want to ensure consistency on first read
                    // For now, assume local 'value' is the source of truth if Redis has no key.
                    // Or, more consistently, if key is not in Redis, it implies 0 for a counter.
                    log.warn("Key {} not found in Redis for currentState. Assuming 0 and attempting to set local value.", redisKey);
                    value = 0L; // Default for a counter if not found
                                // Optionally, set this back to Redis: jedis.set(redisKey, "0");
                }
            } catch (NumberFormatException e) {
                log.error("Redis value for key {} is not a valid Long. Falling back to local counter.", redisKey, e);
                // Fallback to local value below
            } catch (JedisException e) {
                log.error("Redis error in currentState for key {}. Falling back to local counter.", redisKey, e);
                // Fallback to local value below
            } finally {
                RedisConnectionManager.closeJedis(jedis);
            }
        } else {
            log.warn("Could not get Jedis connection for currentState (key {}). Using local counter.", redisKey);
        }
        // Fallback to local counter (either from Redis error, null jedis, or if key not found and not explicitly set to 0)
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("Value", value)};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        long restoredValue = (Long) stateEntry.getValue();
        value = restoredValue; // Always update local value first

        Jedis jedis = RedisConnectionManager.getJedis();
        if (jedis != null) {
            try {
                jedis.set(redisKey, String.valueOf(restoredValue));
                log.info("Successfully restored state for key {} to {} in Redis.", redisKey, restoredValue);
            } catch (JedisException e) {
                log.error("Redis error in restoreState for key {}. State restored to local counter only.", redisKey, e);
                // Local value is already set
            } finally {
                RedisConnectionManager.closeJedis(jedis);
            }
        } else {
            log.warn("Could not get Jedis connection for restoreState (key {}). State restored to local counter only.", redisKey);
        }
    }
}
