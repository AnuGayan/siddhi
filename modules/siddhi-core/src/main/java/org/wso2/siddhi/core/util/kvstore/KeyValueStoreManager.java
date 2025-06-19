package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.util.persistence.RedisConnectionManager; // For shutting down its pool

/**
 * Manages the creation of Key-Value store client instances based on system configuration.
 * This manager determines which type of key-value store (e.g., Redis, Valkey) should be
 * instantiated and used by Siddhi components.
 */
public class KeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStoreManager.class);

    public static final String KEYVALUE_STORE_TYPE_PROPERTY = "siddhi.kvstore.type";
    public static final String REDIS_TYPE = "redis";
    public static final String VALKEY_TYPE = "valkey";
    public static final String DEFAULT_KV_STORE_TYPE = REDIS_TYPE; // Default to Redis

    // To keep track of the type of client last requested, primarily for shutdown.
    // This is a simplification; complex apps might need more sophisticated tracking if multiple types are used.
    private static String lastInitializedClientType = null;

    private KeyValueStoreManager() {
        // Private constructor to prevent instantiation
    }

    /**
     * Gets an instance of a {@link KeyValueStoreClient} based on the system property
     * {@value #KEYVALUE_STORE_TYPE_PROPERTY}.
     * <p>
     * This method reads the system property to determine whether to instantiate a
     * {@link RedisClientAdapter} or a {@link ValkeyClientAdapter}. It then calls the
     * {@code connect()} method on the created adapter instance before returning it.
     * Each call to this method may return a new adapter instance. The adapters
     * themselves are responsible for managing their underlying connection pools or resources.
     * </p>
     *
     * @return A configured and connected {@link KeyValueStoreClient} instance.
     * @throws KeyValueStoreException if the specified store type is unknown or if the
     *                                client adapter fails to connect.
     */
    public static KeyValueStoreClient getClient() {
        String storeType = System.getProperty(KEYVALUE_STORE_TYPE_PROPERTY, DEFAULT_KV_STORE_TYPE).toLowerCase();
        KeyValueStoreClient client;

        log.info("Attempting to initialize KeyValueStoreClient for type: {}", storeType);

        switch (storeType) {
            case REDIS_TYPE:
                client = new RedisClientAdapter();
                break;
            case VALKEY_TYPE:
                client = new ValkeyClientAdapter();
                break;
            default:
                log.error("Unknown key-value store type specified: '{}'. Supported types are '{}' or '{}'.",
                        storeType, REDIS_TYPE, VALKEY_TYPE);
                throw new KeyValueStoreException("Unknown key-value store type: " + storeType);
        }

        try {
            client.connect(); // Initialize connection parameters and connect
            log.info("Successfully created and connected KeyValueStoreClient for type: {}", storeType);
            lastInitializedClientType = storeType; // Track the type for shutdown purposes
        } catch (Exception e) {
            // KeyValueStoreException might already be thrown by adapter's connect() on failure
            log.error("Failed to connect KeyValueStoreClient for type: {}. Reason: {}", storeType, e.getMessage(), e);
            if (e instanceof KeyValueStoreException) {
                throw (KeyValueStoreException) e;
            }
            throw new KeyValueStoreException("Failed to connect KeyValueStoreClient for type: " + storeType, e);
        }
        return client;
    }

    /**
     * Shuts down resources associated with the configured key-value store clients.
     * <p>
     * Currently, this primarily targets the shared pool in {@link RedisConnectionManager}
     * as {@link ValkeyClientAdapter} instances manage their own pools which are closed
     * when their {@code disconnect()} method is called (typically by the consumer of the client).
     * If a specific client type was instantiated via {@code getClient()}, this method attempts
     * to be more targeted if possible, but generally aims to clean up shared resources.
     * </p>
     */
    public static void shutdown() {
        log.info("Shutting down KeyValueStoreManager managed resources.");

        // Shutdown RedisConnectionManager's shared pool, as RedisClientAdapter relies on it.
        // This is always safe to call as RedisConnectionManager's shutdownPool is idempotent.
        try {
            log.info("Attempting to shutdown RedisConnectionManager pool (if used).");
            RedisConnectionManager.shutdownPool();
        } catch (Exception e) {
            log.error("Error during shutdown of RedisConnectionManager pool.", e);
        }

        // For Valkey, ValkeyClientAdapter manages its own JedisPooled instance.
        // If KeyValueStoreManager held a static reference to a Valkey client,
        // it would call client.disconnect() here. But since getClient() returns new instances,
        // the responsibility of calling disconnect() on those instances lies with the caller of getClient().
        // The 'lastInitializedClientType' is more of an indicator, and we can't reliably call disconnect
        // on a specific Valkey instance here without storing that instance.
        // Thus, for Valkey, individual adapter instances must be disconnected by their users.

        if (lastInitializedClientType != null) {
            log.info("Last initialized client type was: {}. Ensure its resources are managed appropriately by the consumer.", lastInitializedClientType);
            lastInitializedClientType = null; // Reset after logging
        }
        log.info("KeyValueStoreManager shutdown process completed.");
    }
}
