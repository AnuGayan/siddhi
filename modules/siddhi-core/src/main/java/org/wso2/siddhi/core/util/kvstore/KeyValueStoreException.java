package org.wso2.siddhi.core.util.kvstore;

/**
 * Custom runtime exception for Key-Value store operations.
 * This exception can be used to wrap underlying exceptions from specific
 * client implementations (e.g., JedisException).
 */
public class KeyValueStoreException extends RuntimeException {

    public KeyValueStoreException(String message) {
        super(message);
    }

    public KeyValueStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
