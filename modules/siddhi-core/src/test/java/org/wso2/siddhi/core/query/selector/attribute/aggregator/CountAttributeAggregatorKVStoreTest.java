package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import io.valkey.JedisPooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.util.ElementIdGenerator;
import org.wso2.siddhi.core.util.kvstore.KVStoreManager;
import org.wso2.siddhi.core.util.kvstore.PooledKVClient;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Map;

public class CountAttributeAggregatorKVStoreTest {

    private CountAttributeAggregator countAttributeAggregator;
    private String baseKVStoreKey;

    private static final String KV_STORE_HOST = "localhost";
    private static final int KV_STORE_PORT = 6379;

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

    @BeforeEach
    public void setUp() {
        // Ensure a clean slate for the KVStoreManager's static singleton before each test.
        KVStoreManager.shutdown();
    }

    private void setSystemProperties(boolean goodConnection) {
        System.setProperty(PooledKVClient.KV_STORE_HOST_PROPERTY, goodConnection ? KV_STORE_HOST : "invalid-kv-host");
        System.setProperty(PooledKVClient.KV_STORE_PORT_PROPERTY, String.valueOf(goodConnection ? KV_STORE_PORT : "1234"));
        // Make sure the singleton is recreated with new properties
        KVStoreManager.shutdown();
    }

    private void initializeMainAggregator() {
        setSystemProperties(true);

        ExecutionPlanContext mainEc = new ExecutionPlanContext();
        mainEc.setName("TestExecutionPlan_Main");
        mainEc.setElementIdGenerator(new TestElementIdGenerator(mainEc.getName()));

        countAttributeAggregator = new CountAttributeAggregator();
        // Aggregator's init will call KVStoreManager.getClient(), which reads the system properties
        countAttributeAggregator.init(null, mainEc);

        baseKVStoreKey = "siddhi:count:" + mainEc.getName() + ":" + "element-0";

        // Verify connection and clean key
        try (JedisPooled directClient = new JedisPooled(KV_STORE_HOST, KV_STORE_PORT)) {
            directClient.del(baseKVStoreKey);
        } catch (Exception e) {
            Assertions.fail("Failed to connect to KV store at " + KV_STORE_HOST + ":" + KV_STORE_PORT + " for setup. Is server running? " + e.getMessage());
        }
    }

    private CountAttributeAggregator initAggregatorForFallbackTest(String planNamePostfix) {
        setSystemProperties(false);

        ExecutionPlanContext fallbackEc = new ExecutionPlanContext();
        fallbackEc.setName("TestExecutionPlan_Fallback_" + planNamePostfix);
        fallbackEc.setElementIdGenerator(new TestElementIdGenerator(fallbackEc.getName()));

        CountAttributeAggregator fallbackAggregator = new CountAttributeAggregator();
        fallbackAggregator.init(null, fallbackEc); // Should use fallback
        return fallbackAggregator;
    }

    @AfterEach
    public void tearDown() {
        if (countAttributeAggregator != null) {
            countAttributeAggregator.stop();
            countAttributeAggregator = null;
        }

        // Clean up system properties
        System.clearProperty(PooledKVClient.KV_STORE_HOST_PROPERTY);
        System.clearProperty(PooledKVClient.KV_STORE_PORT_PROPERTY);

        KVStoreManager.shutdown();

        if (baseKVStoreKey != null) {
            try (JedisPooled directClient = new JedisPooled(KV_STORE_HOST, KV_STORE_PORT)) {
                directClient.del(baseKVStoreKey);
            } catch (Exception e) {
                // Ignore cleanup errors if server is not running
            }
            baseKVStoreKey = null;
        }
    }

    @Test
    public void testGetReturnType() {
        CountAttributeAggregator agg = new CountAttributeAggregator();
        Assertions.assertEquals(Attribute.Type.LONG, agg.getReturnType());
    }

    @Test
    public void testProcessAdd() {
        initializeMainAggregator();
        Assertions.assertEquals(1L, countAttributeAggregator.processAdd(new Object()));
        Assertions.assertEquals(2L, countAttributeAggregator.processAdd(new Object()));
        verifyKeyValue(baseKVStoreKey, "2");
    }

    @Test
    public void testProcessRemove() {
        initializeMainAggregator();
        setKeyValue(baseKVStoreKey, "5");
        Assertions.assertEquals(4L, countAttributeAggregator.processRemove(new Object()));
        Assertions.assertEquals(3L, countAttributeAggregator.processRemove(new Object()));
        verifyKeyValue(baseKVStoreKey, "3");
    }

    @Test
    public void testReset() {
        initializeMainAggregator();
        setKeyValue(baseKVStoreKey, "10");
        Assertions.assertEquals(0L, countAttributeAggregator.reset());
        verifyKeyValue(baseKVStoreKey, "0");
    }

    @Test
    public void testCurrentState() {
        initializeMainAggregator();
        setKeyValue(baseKVStoreKey, "7");
        Object[] state = countAttributeAggregator.currentState();
        Assertions.assertNotNull(state);
        Assertions.assertEquals(1, state.length);
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        Assertions.assertEquals("Value", stateEntry.getKey());
        Assertions.assertEquals(7L, stateEntry.getValue());
    }

    @Test
    public void testRestoreState() {
        initializeMainAggregator();
        Object[] stateToRestore = new Object[]{new java.util.AbstractMap.SimpleEntry<>("Value", 15L)};
        countAttributeAggregator.restoreState(stateToRestore);
        verifyKeyValue(baseKVStoreKey, "15");
        Object[] currentState = countAttributeAggregator.currentState();
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) currentState[0];
        Assertions.assertEquals(15L, stateEntry.getValue());
    }

    @Test
    public void testProcessAddFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("ProcessAdd");
        String potentialKVKey = "siddhi:count:TestExecutionPlan_Fallback_ProcessAdd:element-0";
        Assertions.assertEquals(1L, fallbackAggregator.processAdd(new Object()));
        Assertions.assertEquals(2L, fallbackAggregator.processAdd(new Object()));
        verifyKeyDoesNotExist(potentialKVKey);
        fallbackAggregator.stop();
    }

    @Test
    public void testProcessRemoveFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("ProcessRemove");
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());
        Assertions.assertEquals(2L, fallbackAggregator.processRemove(new Object()));
        Assertions.assertEquals(1L, fallbackAggregator.processRemove(new Object()));
        fallbackAggregator.stop();
    }

    @Test
    public void testResetFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("Reset");
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());
        Assertions.assertEquals(0L, fallbackAggregator.reset());
        Assertions.assertEquals(1L, fallbackAggregator.processAdd(new Object()));
        fallbackAggregator.stop();
    }

    @Test
    public void testCurrentStateFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("CurrentState");
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

    @Test
    public void testRestoreStateFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("RestoreState");
        Object[] stateToRestore = new Object[]{new java.util.AbstractMap.SimpleEntry<>("Value", 25L)};
        fallbackAggregator.restoreState(stateToRestore);
        Object[] currentState = fallbackAggregator.currentState();
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) currentState[0];
        Assertions.assertEquals(25L, stateEntry.getValue());
        fallbackAggregator.stop();
    }

    @Test
    public void testKVStoreKeyUniqueness() {
        setSystemProperties(true);
        ExecutionPlanContext ctx1 = new ExecutionPlanContext();
        ctx1.setName("UniquenessTestPlanA");
        ctx1.setElementIdGenerator(new TestElementIdGenerator(ctx1.getName()));
        CountAttributeAggregator agg1 = new CountAttributeAggregator();
        agg1.init(null, ctx1);
        String key1 = "siddhi:count:UniquenessTestPlanA:element-0";
        deleteKeyValue(key1);
        agg1.processAdd(new Object());
        verifyKeyValue(key1, "1");

        ExecutionPlanContext ctx2 = new ExecutionPlanContext();
        ctx2.setName("UniquenessTestPlanB");
        ctx2.setElementIdGenerator(new TestElementIdGenerator(ctx2.getName()));
        CountAttributeAggregator agg2 = new CountAttributeAggregator();
        agg2.init(null, ctx2);
        String key2 = "siddhi:count:UniquenessTestPlanB:element-0";
        deleteKeyValue(key2);
        agg2.processAdd(new Object());
        agg2.processAdd(new Object());
        verifyKeyValue(key2, "2");
        verifyKeyValue(key1, "1");

        CountAttributeAggregator agg3 = new CountAttributeAggregator();
        agg3.init(null, ctx1);
        String key3 = "siddhi:count:UniquenessTestPlanA:element-1";
        deleteKeyValue(key3);
        agg3.processAdd(new Object());
        agg3.processAdd(new Object());
        agg3.processAdd(new Object());
        verifyKeyValue(key3, "3");
        verifyKeyValue(key1, "1");
        verifyKeyValue(key2, "2");

        deleteKeyValue(key1);
        deleteKeyValue(key2);
        deleteKeyValue(key3);

        agg1.stop();
        agg2.stop();
        agg3.stop();
    }

    @Test
    public void testConcurrentAccessWithSharedPool() {
        setSystemProperties(true);
        ExecutionPlanContext ctxA = new ExecutionPlanContext();
        ctxA.setName("ConcurrencyPlanA");
        ctxA.setElementIdGenerator(new TestElementIdGenerator(ctxA.getName()));
        CountAttributeAggregator aggA = new CountAttributeAggregator();
        aggA.init(null, ctxA);
        String keyA = "siddhi:count:ConcurrencyPlanA:element-0";

        ExecutionPlanContext ctxB = new ExecutionPlanContext();
        ctxB.setName("ConcurrencyPlanB");
        ctxB.setElementIdGenerator(new TestElementIdGenerator(ctxB.getName()));
        CountAttributeAggregator aggB = new CountAttributeAggregator();
        aggB.init(null, ctxB);
        String keyB = "siddhi:count:ConcurrencyPlanB:element-0";

        CountAttributeAggregator aggA_instance2 = new CountAttributeAggregator();
        aggA_instance2.init(null, ctxA);
        String keyA_instance2 = "siddhi:count:ConcurrencyPlanA:element-1";

        deleteKeyValue(keyA);
        deleteKeyValue(keyB);
        deleteKeyValue(keyA_instance2);

        aggA.processAdd(new Object());
        aggB.processAdd(new Object());
        aggA_instance2.processAdd(new Object());
        aggA.processAdd(new Object());
        aggB.processAdd(new Object());
        aggA.processAdd(new Object());
        aggA_instance2.processAdd(new Object());
        aggB.processRemove(new Object());

        verifyKeyValue(keyA, "3");
        verifyKeyValue(keyB, "1");
        verifyKeyValue(keyA_instance2, "2");

        deleteKeyValue(keyA);
        deleteKeyValue(keyB);
        deleteKeyValue(keyA_instance2);

        aggA.stop();
        aggB.stop();
        aggA_instance2.stop();
    }

    // Helper methods for direct KV store interaction for test setup/verification
    private void setKeyValue(String key, String value) {
        try (JedisPooled directClient = new JedisPooled(KV_STORE_HOST, KV_STORE_PORT)) {
            directClient.set(key, value);
        } catch (Exception e) {
            Assertions.fail("Failed to set key " + key + " in KV store for testing: " + e.getMessage(), e);
        }
    }

    private void verifyKeyValue(String key, String expectedValue) {
        try (JedisPooled directClient = new JedisPooled(KV_STORE_HOST, KV_STORE_PORT)) {
            Assertions.assertEquals(expectedValue, directClient.get(key));
        } catch (Exception e) {
            Assertions.fail("Failed to verify key " + key + " in KV store for testing: " + e.getMessage(), e);
        }
    }

    private void verifyKeyDoesNotExist(String key) {
        try (JedisPooled directClient = new JedisPooled(KV_STORE_HOST, KV_STORE_PORT)) {
            Assertions.assertNull(directClient.get(key));
        } catch (Exception e) {
            System.err.println("Could not connect to KV store at " + KV_STORE_HOST + ":" + KV_STORE_PORT + " to verify key non-existence (this may be expected for fallback tests): " + e.getMessage());
        }
    }

    private void deleteKeyValue(String key) {
        try (JedisPooled directClient = new JedisPooled(KV_STORE_HOST, KV_STORE_PORT)) {
            directClient.del(key);
        } catch (Exception e) {
            Assertions.fail("Failed to delete key " + key + " in KV store for testing: " + e.getMessage(), e);
        }
    }
}
