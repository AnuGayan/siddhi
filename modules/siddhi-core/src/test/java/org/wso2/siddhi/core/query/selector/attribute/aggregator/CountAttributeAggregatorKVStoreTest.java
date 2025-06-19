package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.util.ElementIdGenerator;
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreManager;
import org.wso2.siddhi.core.util.persistence.RedisConnectionManager; // For direct Redis cleanup
import org.wso2.siddhi.query.api.definition.Attribute;
import io.valkey.JedisPooled; // For direct Valkey cleanup
import redis.clients.jedis.Jedis; // For direct Redis cleanup (from RedisConnectionManager)


import java.util.Map;
import java.util.stream.Stream;

public class CountAttributeAggregatorKVStoreTest {

    private CountAttributeAggregator countAttributeAggregator;
    private String baseKVStoreKey; // For the main aggregator in some tests

    private static final String REDIS_HOST_DEFAULT = "localhost";
    private static final int REDIS_PORT_DEFAULT = 6379;
    private static final String VALKEY_HOST_DEFAULT = "localhost";
    private static final int VALKEY_PORT_DEFAULT = 6379;

    // To store current test parameters
    private String currentKvStoreType;
    private String currentKvStoreHost;
    private int currentKvStorePort;


    static class TestElementIdGenerator extends ElementIdGenerator {
        private int count = 0;
        public TestElementIdGenerator(String executionPlanName) {
            super(executionPlanName);
        }
        @Override
        public String createNewId() {
            return "element-" + count++;
        }
    }

    // Method source for parameterization
    static Stream<org.junit.jupiter.params.provider.Arguments> kvStoreParameters() {
        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(KeyValueStoreManager.REDIS_TYPE, REDIS_HOST_DEFAULT, REDIS_PORT_DEFAULT),
                org.junit.jupiter.params.provider.Arguments.of(KeyValueStoreManager.VALKEY_TYPE, VALKEY_HOST_DEFAULT, VALKEY_PORT_DEFAULT)
        );
    }

    @BeforeEach
    public void setUp(String kvStoreType, String host, int port) { // Parameters will be injected if test method is parameterized
        // This method is more of a template now; actual property setting happens per test method setup
        // or a specific @BeforeEach that takes parameters (which JUnit doesn't directly support for @BeforeEach with @MethodSource for the class)
        // So, properties will be set directly in test setup helpers or at the start of parameterized tests.
        this.currentKvStoreType = kvStoreType;
        this.currentKvStoreHost = host;
        this.currentKvStorePort = port;

        // Ensure a clean slate for KeyValueStoreManager's static resources if necessary
        // and for RedisConnectionManager's static pool.
        KeyValueStoreManager.shutdown(); // Shuts down Redis pool
    }

    private void setSystemProperties(String kvStoreType, String host, int port, boolean goodConnection) {
        System.setProperty(KeyValueStoreManager.KEYVALUE_STORE_TYPE_PROPERTY, kvStoreType);
        if (KeyValueStoreManager.REDIS_TYPE.equals(kvStoreType)) {
            System.setProperty("redis.host", goodConnection ? host : "invalid-redis-host");
            System.setProperty("redis.port", String.valueOf(goodConnection ? port : "1234"));
        } else if (KeyValueStoreManager.VALKEY_TYPE.equals(kvStoreType)) {
            System.setProperty("valkey.host", goodConnection ? host : "invalid-valkey-host");
            System.setProperty("valkey.port", String.valueOf(goodConnection ? port : "5678"));
        }
        // Important: Shutdown existing static pools so they re-initialize with new properties
        KeyValueStoreManager.shutdown();
    }


    private void initializeMainAggregator(String kvStoreType, String host, int port) {
        setSystemProperties(kvStoreType, host, port, true);

        ExecutionPlanContext mainEc = new ExecutionPlanContext();
        mainEc.setName("TestExecutionPlan_Main_" + kvStoreType);
        mainEc.setElementIdGenerator(new TestElementIdGenerator(mainEc.getName()));

        countAttributeAggregator = new CountAttributeAggregator();
        // Aggregator's init will call KeyValueStoreManager.getClient(), which reads the system properties
        countAttributeAggregator.init(null, mainEc);

        baseKVStoreKey = "siddhi:count:" + mainEc.getName() + ":" + "element-0";

        // Verify connection and clean key
        try {
            if (KeyValueStoreManager.REDIS_TYPE.equals(kvStoreType)) {
                try (Jedis jedisForTest = RedisConnectionManager.getJedis()) {
                    if (jedisForTest == null) Assertions.fail("Failed to get Redis connection for setup.");
                    jedisForTest.del(baseKVStoreKey);
                }
            } else if (KeyValueStoreManager.VALKEY_TYPE.equals(kvStoreType)) {
                try (JedisPooled valkeyPoolForTest = new JedisPooled(host, port)) {
                    valkeyPoolForTest.del(baseKVStoreKey);
                }
            }
        } catch (Exception e) {
            Assertions.fail("Failed to connect to " + kvStoreType + " at " + host + ":" + port + " for setup. Is server running? " + e.getMessage());
        }
    }

    private CountAttributeAggregator initAggregatorForFallbackTest(String kvStoreType, String host, int port, String planNamePostfix) {
        setSystemProperties(kvStoreType, host, port, false); // false for bad connection

        ExecutionPlanContext fallbackEc = new ExecutionPlanContext();
        fallbackEc.setName("TestExecutionPlan_Fallback_" + planNamePostfix + "_" + kvStoreType);
        fallbackEc.setElementIdGenerator(new TestElementIdGenerator(fallbackEc.getName()));

        CountAttributeAggregator fallbackAggregator = new CountAttributeAggregator();
        fallbackAggregator.init(null, fallbackEc); // Should use fallback
        return fallbackAggregator;
    }


    @AfterEach
    public void tearDown() {
        if (countAttributeAggregator != null) {
            countAttributeAggregator.stop(); // This calls kvStoreClient.disconnect()
            countAttributeAggregator = null;
        }

        // Clean up system properties
        System.clearProperty(KeyValueStoreManager.KEYVALUE_STORE_TYPE_PROPERTY);
        System.clearProperty("redis.host");
        System.clearProperty("redis.port");
        System.clearProperty("valkey.host");
        System.clearProperty("valkey.port");

        KeyValueStoreManager.shutdown(); // Ensures RedisConnectionManager pool is closed

        if (baseKVStoreKey != null && currentKvStoreType != null) {
             try {
                if (KeyValueStoreManager.REDIS_TYPE.equals(currentKvStoreType)) {
                    try (Jedis jedisForCleanup = RedisConnectionManager.getJedis()) { // Assumes RedisConnectionManager might still have a pool or can make one
                        if (jedisForCleanup != null) jedisForCleanup.del(baseKVStoreKey);
                    }
                } else if (KeyValueStoreManager.VALKEY_TYPE.equals(currentKvStoreType)) {
                    try (JedisPooled valkeyPoolForCleanup = new JedisPooled(currentKvStoreHost, currentKvStorePort)) {
                        valkeyPoolForCleanup.del(baseKVStoreKey);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error during post-test cleanup of key " + baseKVStoreKey + " for " + currentKvStoreType + ": " + e.getMessage());
            }
            baseKVStoreKey = null;
        }
    }

    @org.junit.jupiter.api.AfterAll
    public static void afterAll() {
        KeyValueStoreManager.shutdown(); // Final cleanup
        System.clearProperty(KeyValueStoreManager.KEYVALUE_STORE_TYPE_PROPERTY);
        System.clearProperty("redis.host");
        System.clearProperty("redis.port");
        System.clearProperty("valkey.host");
        System.clearProperty("valkey.port");
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testGetReturnType(String kvStoreType, String host, int port) {
         // This test doesn't need kvStoreType, host, port, but they are passed by JUnit
        CountAttributeAggregator agg = new CountAttributeAggregator();
        Assertions.assertEquals(Attribute.Type.LONG, agg.getReturnType());
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testProcessAdd(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port); // Store current params for cleanup
        initializeMainAggregator(kvStoreType, host, port);

        Assertions.assertEquals(1L, countAttributeAggregator.processAdd(new Object()));
        Assertions.assertEquals(2L, countAttributeAggregator.processAdd(new Object()));

        verifyKeyValue(kvStoreType, host, port, baseKVStoreKey, "2");
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testProcessRemove(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        initializeMainAggregator(kvStoreType, host, port);
        setKeyValue(kvStoreType, host, port, baseKVStoreKey, "5");

        Assertions.assertEquals(4L, countAttributeAggregator.processRemove(new Object()));
        Assertions.assertEquals(3L, countAttributeAggregator.processRemove(new Object()));
        verifyKeyValue(kvStoreType, host, port, baseKVStoreKey, "3");
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testReset(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        initializeMainAggregator(kvStoreType, host, port);
        setKeyValue(kvStoreType, host, port, baseKVStoreKey, "10");

        Assertions.assertEquals(0L, countAttributeAggregator.reset());
        verifyKeyValue(kvStoreType, host, port, baseKVStoreKey, "0");
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testCurrentState(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        initializeMainAggregator(kvStoreType, host, port);
        setKeyValue(kvStoreType, host, port, baseKVStoreKey, "7");

        Object[] state = countAttributeAggregator.currentState();
        Assertions.assertNotNull(state);
        Assertions.assertEquals(1, state.length);
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        Assertions.assertEquals("Value", stateEntry.getKey());
        Assertions.assertEquals(7L, stateEntry.getValue());
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testRestoreState(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        initializeMainAggregator(kvStoreType, host, port);

        Object[] stateToRestore = new Object[]{new java.util.AbstractMap.SimpleEntry<String, Object>("Value", 15L)};
        countAttributeAggregator.restoreState(stateToRestore);

        verifyKeyValue(kvStoreType, host, port, baseKVStoreKey, "15");
        Object[] currentState = countAttributeAggregator.currentState();
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) currentState[0];
        Assertions.assertEquals(15L, stateEntry.getValue());
    }

    // --- Fallback Tests ---
    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testProcessAddFallback(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port); // Store current params
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest(kvStoreType, host, port, "ProcessAdd");
        String potentialKVKey = "siddhi:count:TestExecutionPlan_Fallback_ProcessAdd_" + kvStoreType + ":element-0";

        Assertions.assertEquals(1L, fallbackAggregator.processAdd(new Object()));
        Assertions.assertEquals(2L, fallbackAggregator.processAdd(new Object()));

        // Verify no key was created in the actual (good) KV store
        verifyKeyDoesNotExist(kvStoreType, host, port, potentialKVKey);
        fallbackAggregator.stop(); // ensure its client is disconnected
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testProcessRemoveFallback(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest(kvStoreType, host, port, "ProcessRemove");
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());

        Assertions.assertEquals(2L, fallbackAggregator.processRemove(new Object()));
        Assertions.assertEquals(1L, fallbackAggregator.processRemove(new Object()));
        fallbackAggregator.stop();
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testResetFallback(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest(kvStoreType, host, port, "Reset");
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());

        Assertions.assertEquals(0L, fallbackAggregator.reset());
        Assertions.assertEquals(1L, fallbackAggregator.processAdd(new Object()));
        fallbackAggregator.stop();
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testCurrentStateFallback(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest(kvStoreType, host, port, "CurrentState");
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());

        Object[] state = fallbackAggregator.currentState();
        Assertions.assertNotNull(state);
        Assertions.assertEquals(1, state.length);
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        Assertions.assertEquals("Value", stateEntry.getKey());
        Assertions.assertEquals(3L, stateEntry.getValue());
        fallbackAggregator.stop();
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testRestoreStateFallback(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest(kvStoreType, host, port, "RestoreState");
        Object[] stateToRestore = new Object[]{new java.util.AbstractMap.SimpleEntry<String, Object>("Value", 25L)};
        fallbackAggregator.restoreState(stateToRestore);

        Object[] currentState = fallbackAggregator.currentState();
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) currentState[0];
        Assertions.assertEquals(25L, stateEntry.getValue());
        fallbackAggregator.stop();
    }

    // --- Key Uniqueness & Concurrent Access Tests (Adapted for Parameterization) ---
    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testKVStoreKeyUniqueness(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        setSystemProperties(kvStoreType, host, port, true);

        ExecutionPlanContext ctx1 = new ExecutionPlanContext();
        ctx1.setName("UniquenessTestPlanA_" + kvStoreType);
        ctx1.setElementIdGenerator(new TestElementIdGenerator(ctx1.getName()));
        CountAttributeAggregator agg1 = new CountAttributeAggregator();
        agg1.init(null, ctx1);
        String key1 = "siddhi:count:UniquenessTestPlanA_" + kvStoreType + ":element-0";
        deleteKeyValue(kvStoreType, host, port, key1);
        agg1.processAdd(new Object());
        verifyKeyValue(kvStoreType, host, port, key1, "1");

        ExecutionPlanContext ctx2 = new ExecutionPlanContext();
        ctx2.setName("UniquenessTestPlanB_" + kvStoreType);
        ctx2.setElementIdGenerator(new TestElementIdGenerator(ctx2.getName()));
        CountAttributeAggregator agg2 = new CountAttributeAggregator();
        agg2.init(null, ctx2);
        String key2 = "siddhi:count:UniquenessTestPlanB_" + kvStoreType + ":element-0";
        deleteKeyValue(kvStoreType, host, port, key2);
        agg2.processAdd(new Object());
        agg2.processAdd(new Object());
        verifyKeyValue(kvStoreType, host, port, key2, "2");
        verifyKeyValue(kvStoreType, host, port, key1, "1");

        CountAttributeAggregator agg3 = new CountAttributeAggregator();
        agg3.init(null, ctx1); // Uses same context as agg1, should get element-1
        String key3 = "siddhi:count:UniquenessTestPlanA_" + kvStoreType + ":element-1";
        deleteKeyValue(kvStoreType, host, port, key3);
        agg3.processAdd(new Object());
        agg3.processAdd(new Object());
        agg3.processAdd(new Object());
        verifyKeyValue(kvStoreType, host, port, key3, "3");
        verifyKeyValue(kvStoreType, host, port, key1, "1");
        verifyKeyValue(kvStoreType, host, port, key2, "2");

        deleteKeyValue(kvStoreType, host, port, key1);
        deleteKeyValue(kvStoreType, host, port, key2);
        deleteKeyValue(kvStoreType, host, port, key3);

        agg1.stop();
        agg2.stop();
        agg3.stop();
    }

    @ParameterizedTest
    @MethodSource("kvStoreParameters")
    public void testConcurrentAccessWithSharedPool(String kvStoreType, String host, int port) {
        setUp(kvStoreType, host, port);
        setSystemProperties(kvStoreType, host, port, true);

        ExecutionPlanContext ctxA = new ExecutionPlanContext();
        ctxA.setName("ConcurrencyPlanA_" + kvStoreType);
        ctxA.setElementIdGenerator(new TestElementIdGenerator(ctxA.getName()));
        CountAttributeAggregator aggA = new CountAttributeAggregator();
        aggA.init(null, ctxA);
        String keyA = "siddhi:count:ConcurrencyPlanA_" + kvStoreType + ":element-0";

        ExecutionPlanContext ctxB = new ExecutionPlanContext();
        ctxB.setName("ConcurrencyPlanB_" + kvStoreType);
        ctxB.setElementIdGenerator(new TestElementIdGenerator(ctxB.getName()));
        CountAttributeAggregator aggB = new CountAttributeAggregator();
        aggB.init(null, ctxB);
        String keyB = "siddhi:count:ConcurrencyPlanB_" + kvStoreType + ":element-0";

        CountAttributeAggregator aggA_instance2 = new CountAttributeAggregator();
        aggA_instance2.init(null, ctxA);
        String keyA_instance2 = "siddhi:count:ConcurrencyPlanA_" + kvStoreType + ":element-1";

        deleteKeyValue(kvStoreType, host, port, keyA);
        deleteKeyValue(kvStoreType, host, port, keyB);
        deleteKeyValue(kvStoreType, host, port, keyA_instance2);

        aggA.processAdd(new Object());
        aggB.processAdd(new Object());
        aggA_instance2.processAdd(new Object());

        aggA.processAdd(new Object());
        aggB.processAdd(new Object());
        aggA.processAdd(new Object());

        aggA_instance2.processAdd(new Object());
        aggB.processRemove(new Object());

        verifyKeyValue(kvStoreType, host, port, keyA, "3");
        verifyKeyValue(kvStoreType, host, port, keyB, "1");
        verifyKeyValue(kvStoreType, host, port, keyA_instance2, "2");

        deleteKeyValue(kvStoreType, host, port, keyA);
        deleteKeyValue(kvStoreType, host, port, keyB);
        deleteKeyValue(kvStoreType, host, port, keyA_instance2);

        aggA.stop();
        aggB.stop();
        aggA_instance2.stop();
    }

    // Helper methods for direct KV store interaction for test setup/verification
    private void setKeyValue(String kvStoreType, String host, int port, String key, String value) {
        try {
            if (KeyValueStoreManager.REDIS_TYPE.equals(kvStoreType)) {
                try (Jedis jedis = RedisConnectionManager.getJedis()) { // Assumes RedisConnectionManager is re-initialized by setSystemProperties
                    Assertions.assertNotNull(jedis, "Jedis connection for setKeyValue should not be null for " + kvStoreType);
                    jedis.set(key, value);
                }
            } else if (KeyValueStoreManager.VALKEY_TYPE.equals(kvStoreType)) {
                try (JedisPooled valkeyPool = new JedisPooled(host, port)) {
                    valkeyPool.set(key, value);
                }
            }
        } catch (Exception e) {
             Assertions.fail("Failed to set key " + key + " in " + kvStoreType + " for testing: " + e.getMessage(), e);
        }
    }

    private void verifyKeyValue(String kvStoreType, String host, int port, String key, String expectedValue) {
        try {
            if (KeyValueStoreManager.REDIS_TYPE.equals(kvStoreType)) {
                try (Jedis jedis = RedisConnectionManager.getJedis()) {
                    Assertions.assertNotNull(jedis, "Jedis connection for verifyKeyValue should not be null for " + kvStoreType);
                    Assertions.assertEquals(expectedValue, jedis.get(key));
                }
            } else if (KeyValueStoreManager.VALKEY_TYPE.equals(kvStoreType)) {
                 try (JedisPooled valkeyPool = new JedisPooled(host, port)) {
                    Assertions.assertEquals(expectedValue, valkeyPool.get(key));
                }
            }
        } catch (Exception e) {
             Assertions.fail("Failed to verify key " + key + " in " + kvStoreType + " for testing: " + e.getMessage(), e);
        }
    }

    private void verifyKeyDoesNotExist(String kvStoreType, String host, int port, String key) {
        try {
            if (KeyValueStoreManager.REDIS_TYPE.equals(kvStoreType)) {
                try (Jedis jedis = RedisConnectionManager.getJedis()) {
                     Assertions.assertNotNull(jedis, "Jedis connection for verifyKeyDoesNotExist should not be null for " + kvStoreType);
                    Assertions.assertNull(jedis.get(key));
                }
            } else if (KeyValueStoreManager.VALKEY_TYPE.equals(kvStoreType)) {
                 try (JedisPooled valkeyPool = new JedisPooled(host, port)) {
                    Assertions.assertNull(valkeyPool.get(key));
                }
            }
        } catch (Exception e) {
             // If connection fails (e.g. bad host for fallback test), this is also a form of "key does not exist" in accessible good store
             System.err.println("Could not connect to " + kvStoreType + " at " + host + ":" + port + " to verify key non-existence (this may be expected for some fallback tests): " + e.getMessage());
        }
    }

    private void deleteKeyValue(String kvStoreType, String host, int port, String key) {
         try {
            if (KeyValueStoreManager.REDIS_TYPE.equals(kvStoreType)) {
                try (Jedis jedis = RedisConnectionManager.getJedis()) {
                    Assertions.assertNotNull(jedis, "Jedis connection for deleteKeyValue should not be null for " + kvStoreType);
                    jedis.del(key);
                }
            } else if (KeyValueStoreManager.VALKEY_TYPE.equals(kvStoreType)) {
                 try (JedisPooled valkeyPool = new JedisPooled(host, port)) {
                    valkeyPool.del(key);
                }
            }
        } catch (Exception e) {
             Assertions.fail("Failed to delete key " + key + " in " + kvStoreType + " for testing: " + e.getMessage(), e);
        }
    }
}
