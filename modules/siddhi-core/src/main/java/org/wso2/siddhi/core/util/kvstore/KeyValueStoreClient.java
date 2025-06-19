package org.wso2.siddhi.core.util.kvstore;

/**
 * Interface for a generic Key-Value store client.
 * This interface defines common operations for interacting with various key-value datastores
 * such as Redis, Valkey, or others that might be used for state management or other purposes
 * within Siddhi.
 */
public interface KeyValueStoreClient {

    /**
     * Establishes a connection to the key-value store.
     * Depending on the implementation, this might initialize a connection pool or
     * perform other setup tasks required to interact with the store.
     * It's recommended to call this method before performing any operations if explicit
     * connection management is required by the underlying client.
     */
    void connect();

    /**
     * Disconnects from the key-value store and releases any associated resources.
     * This could involve closing active connections, shutting down a connection pool,
     * or other cleanup tasks. It should be called when the client is no longer needed,
     * for example, during application shutdown.
     */
    void disconnect();

    /**
     * Checks if the client is currently connected or able to establish a connection
     * to the key-value store.
     * For pooled connections, this might indicate if the pool is healthy and can vend connections.
     *
     * @return {@code true} if the client is connected or can connect, {@code false} otherwise.
     */
    boolean isConnected();

    /**
     * Retrieves the string value associated with the given key.
     *
     * @param key The key whose associated value is to be returned.
     * @return The string value to which the specified key is mapped, or {@code null}
     *         if this store contains no mapping for the key or if the key itself is null.
     */
    String get(String key);

    /**
     * Sets the string value for the given key.
     * If the store previously contained a mapping for the key, the old value is replaced by
     * the specified value.
     *
     * @param key   The key with which the specified value is to be associated.
     * @param value The value to be associated with the specified key.
     */
    void set(String key, String value);

    /**
     * Increments the numeric value of a key by one.
     * If the key does not exist, it is set to 0 before performing the operation.
     * If the value stored at the key is not a number or cannot be converted to one,
     * the behavior might be implementation-specific (e.g., throw an exception or return an error state).
     *
     * @param key The key whose numeric value is to be incremented.
     * @return The value of the key after the increment operation.
     */
    long increment(String key);

    /**
     * Decrements the numeric value of a key by one.
     * If the key does not exist, it is set to 0 before performing the operation.
     * If the value stored at the key is not a number or cannot be converted to one,
     * the behavior might be implementation-specific (e.g., throw an exception or return an error state).
     *
     * @param key The key whose numeric value is to be decremented.
     * @return The value of the key after the decrement operation.
     */
    long decrement(String key);

    /**
     * Deletes the mapping for a key from this store if it is present.
     * If there is no mapping for the key, this method has no effect.
     *
     * @param key The key whose mapping is to be removed from the store.
     */
    void delete(String key);
}
