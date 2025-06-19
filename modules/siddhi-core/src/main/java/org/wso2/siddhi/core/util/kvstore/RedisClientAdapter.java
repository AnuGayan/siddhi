package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.util.persistence.RedisConnectionManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * An adapter implementation of {@link KeyValueStoreClient} for Redis.
 * This class uses the shared {@link RedisConnectionManager} to interact with a Redis server.
 */
public class RedisClientAdapter implements KeyValueStoreClient {

    private static final Logger log = LoggerFactory.getLogger(RedisClientAdapter.class);

    public RedisClientAdapter() {
        // Constructor can be lightweight. Connection management is handled by RedisConnectionManager.
    }

    @Override
    public void connect() {
        // The RedisConnectionManager initializes the pool on first use.
        // We can try to get a connection here to ensure the pool is alive or to log.
        log.info("Attempting to ensure Redis connection pool is initialized.");
        Jedis jedis = null;
        try {
            jedis = RedisConnectionManager.getJedis();
            if (jedis != null) {
                log.info("Successfully obtained a connection from Redis pool. Connection appears to be available.");
            } else {
                log.warn("Failed to obtain a connection from Redis pool during connect(). Pool might be uninitialized or Redis unavailable.");
            }
        } catch (Exception e) {
            log.error("Error while trying to establish initial connection to Redis via pool.", e);
            // Do not throw KeyValueStoreException here as connect() is void and might be called proactively.
        } finally {
            if (jedis != null) {
                RedisConnectionManager.closeJedis(jedis);
            }
        }
    }

    @Override
    public void disconnect() {
        log.info("Disconnecting from Redis: Shutting down the shared Redis connection pool.");
        try {
            RedisConnectionManager.shutdownPool();
            log.info("Redis connection pool shutdown successful.");
        } catch (Exception e) {
            // Log and wrap in a runtime exception if necessary, though shutdownPool usually logs its own errors.
            log.error("Error encountered while shutting down Redis connection pool.", e);
            throw new KeyValueStoreException("Error shutting down Redis connection pool", e);
        }
    }

    @Override
    public boolean isConnected() {
        Jedis jedis = null;
        try {
            jedis = RedisConnectionManager.getJedis();
            if (jedis != null) {
                String pong = jedis.ping();
                return "PONG".equalsIgnoreCase(pong);
            }
            return false;
        } catch (JedisException e) {
            log.warn("Failed to ping Redis server or get connection for isConnected check.", e);
            return false;
        } finally {
            if (jedis != null) {
                RedisConnectionManager.closeJedis(jedis);
            }
        }
    }

    @Override
    public String get(String key) {
        if (key == null) {
            log.warn("Get operation called with null key. Returning null.");
            return null;
        }
        Jedis jedis = null;
        try {
            jedis = RedisConnectionManager.getJedis();
            if (jedis == null) {
                log.error("Cannot perform GET for key '{}': Jedis instance is null (pool unavailable or uninitialized).", key);
                throw new KeyValueStoreException("Failed to get connection from Redis pool for GET operation.");
            }
            return jedis.get(key);
        } catch (JedisException e) {
            log.error("JedisException during GET for key '{}'.", key, e);
            throw new KeyValueStoreException("Error during Redis GET for key: " + key, e);
        } finally {
            if (jedis != null) {
                RedisConnectionManager.closeJedis(jedis);
            }
        }
    }

    @Override
    public void set(String key, String value) {
        if (key == null) {
            log.error("SET operation called with null key. Operation aborted.");
            throw new KeyValueStoreException("Key cannot be null for SET operation.");
        }
        if (value == null) {
            log.warn("SET operation called with null value for key '{}'. Behavior might depend on underlying store (e.g., Redis stores empty string).", key);
            // Forcing non-null for consistency, though Redis handles nulls by not setting or erroring depending on client.
            // Jedis client might throw if value is null for certain commands or convert it.
            // Let's be explicit: if a user wants to store "null-like", they should use an empty string or specific marker.
            // However, the interface is String value, so we allow it but Jedis might throw.
            // Let's assume Jedis client handles it or caller ensures non-null if critical.
            // For now, proceed, and let Jedis handle it.
        }
        Jedis jedis = null;
        try {
            jedis = RedisConnectionManager.getJedis();
            if (jedis == null) {
                log.error("Cannot perform SET for key '{}': Jedis instance is null (pool unavailable or uninitialized).", key);
                throw new KeyValueStoreException("Failed to get connection from Redis pool for SET operation.");
            }
            jedis.set(key, value);
        } catch (JedisException e) {
            log.error("JedisException during SET for key '{}'.", key, e);
            throw new KeyValueStoreException("Error during Redis SET for key: " + key, e);
        } finally {
            if (jedis != null) {
                RedisConnectionManager.closeJedis(jedis);
            }
        }
    }

    @Override
    public long increment(String key) {
        if (key == null) {
            log.error("INCREMENT operation called with null key. Operation aborted.");
            throw new KeyValueStoreException("Key cannot be null for INCREMENT operation.");
        }
        Jedis jedis = null;
        try {
            jedis = RedisConnectionManager.getJedis();
            if (jedis == null) {
                log.error("Cannot perform INCREMENT for key '{}': Jedis instance is null (pool unavailable or uninitialized).", key);
                throw new KeyValueStoreException("Failed to get connection from Redis pool for INCREMENT operation.");
            }
            return jedis.incr(key);
        } catch (JedisException e) {
            log.error("JedisException during INCREMENT for key '{}'.", key, e);
            throw new KeyValueStoreException("Error during Redis INCREMENT for key: " + key, e);
        } finally {
            if (jedis != null) {
                RedisConnectionManager.closeJedis(jedis);
            }
        }
    }

    @Override
    public long decrement(String key) {
        if (key == null) {
            log.error("DECREMENT operation called with null key. Operation aborted.");
            throw new KeyValueStoreException("Key cannot be null for DECREMENT operation.");
        }
        Jedis jedis = null;
        try {
            jedis = RedisConnectionManager.getJedis();
            if (jedis == null) {
                log.error("Cannot perform DECREMENT for key '{}': Jedis instance is null (pool unavailable or uninitialized).", key);
                throw new KeyValueStoreException("Failed to get connection from Redis pool for DECREMENT operation.");
            }
            return jedis.decr(key);
        } catch (JedisException e) {
            log.error("JedisException during DECREMENT for key '{}'.", key, e);
            throw new KeyValueStoreException("Error during Redis DECREMENT for key: " + key, e);
        } finally {
            if (jedis != null) {
                RedisConnectionManager.closeJedis(jedis);
            }
        }
    }

    @Override
    public void delete(String key) {
        if (key == null) {
            log.error("DELETE operation called with null key. Operation aborted.");
            throw new KeyValueStoreException("Key cannot be null for DELETE operation.");
        }
        Jedis jedis = null;
        try {
            jedis = RedisConnectionManager.getJedis();
            if (jedis == null) {
                log.error("Cannot perform DELETE for key '{}': Jedis instance is null (pool unavailable or uninitialized).", key);
                throw new KeyValueStoreException("Failed to get connection from Redis pool for DELETE operation.");
            }
            jedis.del(key);
        } catch (JedisException e) {
            log.error("JedisException during DELETE for key '{}'.", key, e);
            throw new KeyValueStoreException("Error during Redis DELETE for key: " + key, e);
        } finally {
            if (jedis != null) {
                RedisConnectionManager.closeJedis(jedis);
            }
        }
    }
}
