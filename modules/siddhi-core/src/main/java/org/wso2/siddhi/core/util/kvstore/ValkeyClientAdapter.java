package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.valkey.JedisPooled; // Assuming this is the correct import for pooled operations
import io.valkey.exceptions.ValkeyException; // Assuming this is the base exception class

/**
 * An adapter implementation of {@link KeyValueStoreClient} for Valkey.
 * This class uses {@link JedisPooled} from the valkey-java client library,
 * which provides built-in connection pooling.
 */
public class ValkeyClientAdapter implements KeyValueStoreClient {

    private static final Logger log = LoggerFactory.getLogger(ValkeyClientAdapter.class);

    private static final String VALKEY_HOST_PROPERTY = "valkey.host";
    private static final String VALKEY_PORT_PROPERTY = "valkey.port";
    private static final String DEFAULT_VALKEY_HOST = "localhost";
    private static final int DEFAULT_VALKEY_PORT = 6379; // Standard Valkey/Redis port

    private JedisPooled valkeyPool;
    private String host;
    private int port;
    private boolean initialized = false;

    public ValkeyClientAdapter() {
        // Configuration will be read during connect()
    }

    @Override
    public void connect() {
        if (initialized) {
            log.info("ValkeyClientAdapter is already initialized for {}:{}.", host, port);
            return;
        }
        this.host = System.getProperty(VALKEY_HOST_PROPERTY, DEFAULT_VALKEY_HOST);
        String portStr = System.getProperty(VALKEY_PORT_PROPERTY, String.valueOf(DEFAULT_VALKEY_PORT));
        try {
            this.port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            log.warn("Invalid Valkey port '{}' specified in system property '{}'. Using default port: {}.",
                    portStr, VALKEY_PORT_PROPERTY, DEFAULT_VALKEY_PORT, e);
            this.port = DEFAULT_VALKEY_PORT;
        }

        log.info("Attempting to connect to Valkey server at {}:{}", host, port);
        try {
            this.valkeyPool = new JedisPooled(this.host, this.port);
            // Perform a quick test operation like PING to ensure connection is truly established
            String pingResponse = this.valkeyPool.ping();
            if ("PONG".equalsIgnoreCase(pingResponse)) {
                log.info("Successfully connected to Valkey server at {}:{} and received PONG.", host, port);
                this.initialized = true;
            } else {
                log.warn("Connected to Valkey server at {}:{} but PING response was unexpected: {}", host, port, pingResponse);
                // Consider it connected if no exception, but log the unexpected PONG
                this.initialized = true;
            }
        } catch (ValkeyException e) {
            log.error("Failed to initialize Valkey connection pool for server at {}:{}.", host, port, e);
            this.valkeyPool = null; // Ensure pool is null if connection failed
            this.initialized = false;
            throw new KeyValueStoreException("Failed to connect to Valkey server at " + host + ":" + port, e);
        }
    }

    @Override
    public void disconnect() {
        log.info("Disconnecting from Valkey server at {}:{}.", host, port);
        if (this.valkeyPool != null) {
            try {
                this.valkeyPool.close();
                log.info("Valkey connection pool closed for {}:{}.", host, port);
            } catch (Exception e) { // JedisPooled.close() might not declare specific exceptions
                log.error("Error encountered while closing Valkey connection pool for {}:{}.", host, port, e);
                throw new KeyValueStoreException("Error closing Valkey connection pool", e);
            } finally {
                this.valkeyPool = null;
                this.initialized = false;
            }
        } else {
            log.info("Valkey connection pool was already null or not initialized for {}:{}.", host, port);
            this.initialized = false; // Ensure consistent state
        }
    }

    @Override
    public boolean isConnected() {
        if (this.valkeyPool == null || !this.initialized) {
            return false;
        }
        try {
            String pong = valkeyPool.ping();
            return "PONG".equalsIgnoreCase(pong);
        } catch (ValkeyException e) {
            log.warn("Failed to ping Valkey server at {}:{}. Considering disconnected.", host, port, e);
            return false;
        }
    }

    private void checkConnected() {
        if (valkeyPool == null || !this.initialized) {
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
            return valkeyPool.get(key);
        } catch (ValkeyException e) {
            log.error("ValkeyException during GET for key '{}' from {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during Valkey GET for key: " + key, e);
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
            valkeyPool.set(key, value);
        } catch (ValkeyException e) {
            log.error("ValkeyException during SET for key '{}' to {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during Valkey SET for key: " + key, e);
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
            return valkeyPool.incr(key);
        } catch (ValkeyException e) {
            log.error("ValkeyException during INCREMENT for key '{}' at {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during Valkey INCREMENT for key: " + key, e);
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
            return valkeyPool.decr(key);
        } catch (ValkeyException e) {
            log.error("ValkeyException during DECREMENT for key '{}' at {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during Valkey DECREMENT for key: " + key, e);
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
            valkeyPool.del(key);
        } catch (ValkeyException e) {
            log.error("ValkeyException during DELETE for key '{}' at {}:{}.", key, host, port, e);
            throw new KeyValueStoreException("Error during Valkey DELETE for key: " + key, e);
        }
    }
}
