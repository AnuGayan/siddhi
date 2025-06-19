package org.wso2.siddhi.core.util.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

public class RedisConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(RedisConnectionManager.class);

    private static volatile JedisPool jedisPool;

    private static final String DEFAULT_REDIS_HOST = "localhost";
    private static final int DEFAULT_REDIS_PORT = 6379;

    // Default JedisPool configurations
    private static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 128;
    private static final int DEFAULT_MAX_IDLE_CONNECTIONS = 128;
    private static final int DEFAULT_MIN_IDLE_CONNECTIONS = 16;
    private static final boolean DEFAULT_TEST_ON_BORROW = true;
    private static final boolean DEFAULT_TEST_ON_RETURN = true;
    private static final boolean DEFAULT_TEST_WHILE_IDLE = true;
    private static final long DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 60000L; // 1 minute
    private static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 30000L; // 30 seconds
    private static final int DEFAULT_NUM_TESTS_PER_EVICTION_RUN = 3;
    private static final boolean DEFAULT_BLOCK_WHEN_EXHAUSTED = true;


    private RedisConnectionManager() {
        // Private constructor to prevent instantiation
    }

    public static JedisPool getJedisPool() {
        if (jedisPool == null) {
            synchronized (RedisConnectionManager.class) {
                if (jedisPool == null) {
                    String redisHost = System.getProperty("redis.host", DEFAULT_REDIS_HOST);
                    int redisPort;
                    try {
                        redisPort = Integer.parseInt(System.getProperty("redis.port", String.valueOf(DEFAULT_REDIS_PORT)));
                    } catch (NumberFormatException e) {
                        log.warn("Invalid Redis port specified in system property 'redis.port'. Using default port: {}", DEFAULT_REDIS_PORT, e);
                        redisPort = DEFAULT_REDIS_PORT;
                    }

                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(DEFAULT_MAX_TOTAL_CONNECTIONS);
                    poolConfig.setMaxIdle(DEFAULT_MAX_IDLE_CONNECTIONS);
                    poolConfig.setMinIdle(DEFAULT_MIN_IDLE_CONNECTIONS);
                    poolConfig.setTestOnBorrow(DEFAULT_TEST_ON_BORROW);
                    poolConfig.setTestOnReturn(DEFAULT_TEST_ON_RETURN);
                    poolConfig.setTestWhileIdle(DEFAULT_TEST_WHILE_IDLE);
                    poolConfig.setMinEvictableIdleTimeMillis(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
                    poolConfig.setTimeBetweenEvictionRunsMillis(DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
                    poolConfig.setNumTestsPerEvictionRun(DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
                    poolConfig.setBlockWhenExhausted(DEFAULT_BLOCK_WHEN_EXHAUSTED);

                    try {
                        jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
                        log.info("JedisPool initialized for Redis server at {}:{}", redisHost, redisPort);
                    } catch (Exception e) {
                        log.error("Failed to initialize JedisPool for Redis server at {}:{}", redisHost, redisPort, e);
                        // jedisPool will remain null, subsequent calls to getJedisPool will retry
                    }
                }
            }
        }
        return jedisPool;
    }

    public static Jedis getJedis() {
        JedisPool pool = getJedisPool();
        if (pool != null) {
            try {
                return pool.getResource();
            } catch (JedisException e) {
                log.error("Failed to get Jedis resource from pool. Is Redis server running and accessible?", e);
                // Propagate the exception or return null based on desired error handling strategy.
                // Returning null here means callers must handle it.
                return null;
            }
        }
        log.error("JedisPool is not initialized. Cannot get Jedis resource.");
        return null;
    }

    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close(); // Returns the resource to the pool
            } catch (JedisException e) {
                log.error("Error while returning Jedis resource to pool", e);
                // If jedis.close() throws an exception, the resource might not be returned correctly.
                // Depending on the Jedis version, further action might be needed (e.g., destroying the object).
            }
        }
    }

    public static void shutdownPool() {
        synchronized (RedisConnectionManager.class) {
            if (jedisPool != null && !jedisPool.isClosed()) {
                try {
                    jedisPool.close();
                    log.info("JedisPool has been closed.");
                } catch (Exception e) {
                    log.error("Error while closing JedisPool", e);
                } finally {
                    jedisPool = null; // Allow re-initialization if needed later, though typically not for shutdown
                }
            } else {
                log.info("JedisPool was not initialized or already closed. No action taken for shutdown.");
            }
        }
    }
}
