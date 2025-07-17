package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a singleton instance of a Key-Value store client.
 * This manager ensures that a single, shared client instance is used throughout Siddhi,
 * configured via system properties. The underlying client uses a connection pool for
 * efficient resource management.
 */
public class KVStoreManager {

    private static final Logger log = LoggerFactory.getLogger(KVStoreManager.class);

    // The singleton client instance. 'volatile' ensures visibility across threads.
    private static volatile KeyValueStoreClient clientInstance;

    private KVStoreManager() {
        // Private constructor to prevent instantiation
    }

    /**
     * Gets the singleton instance of the {@link KeyValueStoreClient}.
     * <p>
     * This method uses double-checked locking to ensure that only one instance of
     * {@link PooledKVClient} is created. The client is configured using system
     * properties (e.g., `siddhi.kvstore.host`, `siddhi.kvstore.port`). On first
     * call, it instantiates and connects the client. Subsequent calls return the
     * existing instance.
     * </p>
     *
     * @return The singleton, connected {@link KeyValueStoreClient} instance.
     * @throws KeyValueStoreException if the client fails to connect on initialization.
     */
    public static KeyValueStoreClient getClient() {
        // Use a local variable to reduce volatile reads
        KeyValueStoreClient localInstance = clientInstance;
        if (localInstance == null) {
            synchronized (KVStoreManager.class) {
                localInstance = clientInstance;
                if (localInstance == null) {
                    log.info("No KeyValueStoreClient instance found. Creating a new one.");
                    try {
                        // Instantiate, connect, and assign to the static field.
                        clientInstance = localInstance = new PooledKVClient();
                        localInstance.connect();
                        log.info("Successfully created and connected the singleton KeyValueStoreClient instance.");
                    } catch (KeyValueStoreException e) {
                        log.error("Failed to initialize and connect the singleton KeyValueStoreClient. " +
                                "Subsequent calls will re-attempt.", e);
                        // Ensure clientInstance is not left in a partially constructed state
                        clientInstance = null;
                        throw e; // Re-throw the exception to the caller
                    }
                }
            }
        }
        return localInstance;
    }

    /**
     * Shuts down the singleton Key-Value store client instance.
     * <p>
     * This method disconnects the client and releases its resources (e.g., closes the
     * connection pool). It also sets the internal instance to null, allowing it to be
     * re-created on a subsequent call to {@code getClient()}. This is particularly
     * useful for tests or applications that need to reconfigure and restart services.
     * </p>
     */
    public static void shutdown() {
        synchronized (KVStoreManager.class) {
            if (clientInstance != null) {
                log.info("Shutting down the singleton KeyValueStoreClient instance.");
                try {
                    clientInstance.disconnect();
                } catch (Exception e) {
                    log.error("Error during shutdown of the KeyValueStoreClient instance.", e);
                } finally {
                    clientInstance = null; // Allow re-initialization
                }
            } else {
                log.info("Shutdown called, but no active KeyValueStoreClient instance to shut down.");
            }
        }
    }
}
