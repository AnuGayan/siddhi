package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.valkey.JedisPooled; // Assuming this is the correct import for pooled operations
import io.valkey.exceptions.ValkeyException; // Assuming this is the base exception class

/**
 * A generic Key-Value store client implementation that uses a connection pool.
 * This class uses {@link JedisPooled} from the valkey-java client library, which is
 * compatible with both Redis and Valkey and provides built-in connection pooling.
 */
public class PooledKVClient implements KeyValueStoreClient {

    private static final Logger log = LoggerFactory.getLogger(PooledKVClient.class);

    // Generic properties for configuration
    public static final String KV_STORE_HOST_PROPERTY = "siddhi.kvstore.host";
    public static final String KV_STORE_PORT_PROPERTY = "siddhi.kvstore.port";
    public static final String DEFAULT_KV_STORE_HOST = "localhost";
    public static final int DEFAULT_KV_STORE_PORT = 6379;

    private JedisPooled kvPool;
    private String host;
    private int port;
    private boolean initialized = false;

    public PooledKVClient() {
        // Configuration will be read during connect()
    }

    @Override
    public void connect() {
        if (initialized) {
            log.info("PooledKVClient is already initialized for {}:{}.", host, port);
            return;
        }
        this.host = System.getProperty(KV_STORE_HOST_PROPERTY, DEFAULT_KV_STORE_HOST);
        String portStr = System.getProperty(KV_STORE_PORT_PROPERTY, String.valueOf(DEFAULT_KV_STORE_PORT));
        try {
            this.port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            log.warn("Invalid Key-Value store port '{}' specified in system property '{}'. Using default port: {}.",
                    portStr, KV_STORE_PORT_PROPERTY, DEFAULT_KV_STORE_PORT, e);
            this.port = DEFAULT_KV_STORE_PORT;
        }

        log.info("Attempting to connect to Key-Value store server at {}:{}", host, port);
        try {
            this.kvPool = new JedisPooled(this.host, this.port);
            // Perform a quick test operation like PING to ensure connection is truly established
            String pingResponse = this.kvPool.ping();
            if ("PONG".equalsIgnoreCase(pingResponse)) {
                log.info("Successfully connected to Key-Value store server at {}:{} and received PONG.", host, port);
                this.initialized = true;
            } else {
                log.warn("Connected to Key-Value store server at {}:{} but PING response was unexpected: {}", host, port, pingResponse);
                // Consider it connected if no exception, but log the unexpected PONG
                this.initialized = true;
            }
        } catch (ValkeyException e) {
            log.error("Failed to initialize connection pool for Key-Value store server at {}:{}.", host, port, e);
            this.kvPool = null; // Ensure pool is null if connection failed
            this.initialized = false;
            throw new KeyValueStoreException("Failed to connect to Key-Value store server at " + host + ":" + port, e);
        }
    }

    @Override
    public void disconnect() {
        log.info("Disconnecting from Key-Value store server at {}:{}.", host, port);
        if (this.kvPool != null) {
            try {
                this.kvPool.close();
                log.info("Connection pool closed for {}:{}.", host, port);
            } catch (Exception e) { // JedisPooled.close() might not declare specific exceptions
                log.error("Error encountered while closing connection pool for {}:{}.", host, port, e);
                throw new KeyValueStoreException("Error closing connection pool", e);
            } finally {
                this.kvPool = null;
                this.initialized = false;
            }
        } else {
            log.info("Connection pool was already null or not initialized for {}:{}.", host, port);
            this.initialized = false; // Ensure consistent state
        }
    }

    @Override
    public boolean isConnected() {
        if (this.kvPool == null || !this.initialized) {
            return false;
        }
        try {
            String pong = kvPool.ping();
            return "PONG".equalsIgnoreCase(pong);
        } catch (ValkeyException e) {
            log.warn("Failed to ping Key-Value store server at {}:{}. Considering disconnected.", host, port, e);
            return false;
        }
    }

    private void checkConnected() {
        if (kvPool == null || !this.initialized) {
            throw new KeyValueStoreException("Client not connected. Call connect() first.");
        }
    }

    @Override
    public String get(String key) {
        checkConnected();
        if (key == null) {
            log.warn("GET operation called with null key. Returning null.");
            return null;
        }
        try {
            return kvPool.get(key);
        } catch (ValkeyException e) {
            log.error("Exception during GET for key '{}' from {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during KV store GET for key: " + key, e);
        }
    }

    @Override
    public void set(String key, String value) {
        checkConnected();
        if (key == null) {
            log.error("SET operation called with null key. Operation aborted.");
            throw new KeyValueStoreException("Key cannot be null for SET operation.");
        }
        try {
            kvPool.set(key, value);
        } catch (ValkeyException e) {
            log.error("Exception during SET for key '{}' to {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during KV store SET for key: " + key, e);
        }
    }

    @Override
    public long increment(String key) {
        checkConnected();
        if (key == null) {
            log.error("INCREMENT operation called with null key. Operation aborted.");
            throw new KeyValueStoreException("Key cannot be null for INCREMENT operation.");
        }
        try {
            return kvPool.incr(key);
        } catch (ValkeyException e) {
            log.error("Exception during INCREMENT for key '{}' at {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during KV store INCREMENT for key: " + key, e);
        }
    }

    @Override
    public long decrement(String key) {
        checkConnected();
        if (key == null) {
            log.error("DECREMENT operation called with null key. Operation aborted.");
            throw new KeyValueStoreException("Key cannot be null for DECREMENT operation.");
        }
        try {
            return kvPool.decr(key);
        } catch (ValkeyException e) {
            log.error("Exception during DECREMENT for key '{}' at {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during KV store DECREMENT for key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        checkConnected();
        if (key == null) {
            log.error("DELETE operation called with null key. Operation aborted.");
            throw new KeyValueStoreException("Key cannot be null for DELETE operation.");
        }
        try {
            kvPool.del(key);
        } catch (ValkeyException e) {
            log.error("Exception during DELETE for key '{}' at {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during KV store DELETE for key: " + key, e);
        }
    }
}
