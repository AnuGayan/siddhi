package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.util.ElementIdGenerator;
import org.wso2.siddhi.query.api.definition.Attribute;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class CountAttributeAggregatorRedisTest {

    private CountAttributeAggregator countAttributeAggregator;
    // private ExecutionPlanContext executionPlanContext; // Will be created per test/aggregator as needed
    // private Jedis jedis; // Removed - use RedisConnectionManager
    private final String redisHost = "localhost"; // Default for "good" Redis connection
    private final int redisPort = 6379;
    private String baseRedisKey; // For the main aggregator in some tests

    private class TestElementIdGenerator extends ElementIdGenerator {
        private int count = 0;
        public TestElementIdGenerator(String executionPlanName) {
            super(executionPlanName);
        }
        @Override
        public String createNewId() {
            return "element-" + count++;
        }
    }

    // No @Before needed for general setup if each test configures properties and pool state

    private void setupForGoodRedis() {
        System.setProperty("redis.host", redisHost);
        System.setProperty("redis.port", String.valueOf(redisPort));
        org.wso2.siddhi.core.util.persistence.RedisConnectionManager.shutdownPool(); // Ensure pool re-initializes with these properties
    }

    private void setupForBadRedis() {
        System.setProperty("redis.host", "invalid-redis-host");
        System.setProperty("redis.port", "1234");
        org.wso2.siddhi.core.util.persistence.RedisConnectionManager.shutdownPool(); // Ensure pool re-initializes with these properties
    }


    private void initializeMainAggregatorWithGoodRedis() {
        setupForGoodRedis(); // Sets properties and resets pool

        ExecutionPlanContext mainEc = new ExecutionPlanContext();
        mainEc.setName("TestExecutionPlan_Main");
        mainEc.setElementIdGenerator(new TestElementIdGenerator(mainEc.getName()));

        countAttributeAggregator = new CountAttributeAggregator();
        countAttributeAggregator.init(null, mainEc); // This will trigger RedisConnectionManager.getJedis() -> pool init if needed

        baseRedisKey = "siddhi:count:" + mainEc.getName() + ":" + "element-0";

        // Verify connection and clean key using a new Jedis instance from the pool
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            if (jedisForTest == null) {
                Assert.fail("Failed to get Jedis connection from pool for setup. Redis server might be down.");
            }
            jedisForTest.del(baseRedisKey);
        }
    }

    // Removed ensureRedisConnection() - tests will try to get a connection and fail if not available.

    @org.junit.After
    public void tearDown() {
        // Clean up system properties
        System.clearProperty("redis.host");
        System.clearProperty("redis.port");

        // Shutdown the pool after each test to ensure subsequent tests can reconfigure it
        // This is important because the JedisPool in RedisConnectionManager is static.
        org.wso2.siddhi.core.util.persistence.RedisConnectionManager.shutdownPool();

        // Clean up the specific key used by initializeMainAggregatorWithGoodRedis if it was set
        if (baseRedisKey != null) {
            // Try to get a connection to clean up, but don't fail test if Redis is already down
            // This assumes default host/port for cleanup if specific test properties were bad
            System.setProperty("redis.host", redisHost);
            System.setProperty("redis.port", String.valueOf(redisPort));
            // No need to shutdown pool here, just getting a connection for cleanup
            try (Jedis jedisForCleanup = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
                if (jedisForCleanup != null) {
                    jedisForCleanup.del(baseRedisKey);
                }
            } catch (Exception e) {
                // Ignore cleanup errors, test might have left Redis in a bad state
            }
            baseRedisKey = null; // Reset for next test
        }
    }

    @org.junit.AfterClass
    public static void afterClass() {
        // Final shutdown of the pool after all tests in the class have run.
        org.wso2.siddhi.core.util.persistence.RedisConnectionManager.shutdownPool();
        System.clearProperty("redis.host");
        System.clearProperty("redis.port");
    }

    @Test
    public void testGetReturnType() {
        // getReturnType does not depend on Redis or init state, so can be tested simply
        CountAttributeAggregator agg = new CountAttributeAggregator();
        Assert.assertEquals(Attribute.Type.LONG, agg.getReturnType());
    }

    @Test
    public void testProcessAddWithRedis() {
        initializeMainAggregatorWithGoodRedis(); // Sets up countAttributeAggregator and baseRedisKey

        Assert.assertEquals(1L, countAttributeAggregator.processAdd(new Object()));
        Assert.assertEquals(2L, countAttributeAggregator.processAdd(new Object()));
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull("Jedis connection should be available for verification", jedisForTest);
            Assert.assertEquals("2", jedisForTest.get(baseRedisKey));
        }
    }

    @Test
    public void testProcessRemoveWithRedis() {
        initializeMainAggregatorWithGoodRedis();

        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull("Jedis connection should be available for verification", jedisForTest);
            jedisForTest.set(baseRedisKey, "5"); // Start with a known value
        }

        Assert.assertEquals(4L, countAttributeAggregator.processRemove(new Object()));
        Assert.assertEquals(3L, countAttributeAggregator.processRemove(new Object()));
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull("Jedis connection should be available for verification", jedisForTest);
            Assert.assertEquals("3", jedisForTest.get(baseRedisKey));
        }
    }

    @Test
    public void testResetWithRedis() {
        initializeMainAggregatorWithGoodRedis();
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull("Jedis connection should be available for verification", jedisForTest);
            jedisForTest.set(baseRedisKey, "10");
        }

        Assert.assertEquals(0L, countAttributeAggregator.reset());
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull("Jedis connection should be available for verification", jedisForTest);
            Assert.assertEquals("0", jedisForTest.get(baseRedisKey));
        }
    }

    @Test
    public void testCurrentStateWithRedis() {
        initializeMainAggregatorWithGoodRedis();
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull("Jedis connection should be available for verification", jedisForTest);
            jedisForTest.set(baseRedisKey, "7");
        }

        Object[] state = countAttributeAggregator.currentState();
        Assert.assertNotNull(state);
        Assert.assertEquals(1, state.length);
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        Assert.assertEquals("Value", stateEntry.getKey());
        Assert.assertEquals(7L, stateEntry.getValue());
    }

    @Test
    public void testRestoreStateWithRedis() {
        initializeMainAggregatorWithGoodRedis();

        Object[] stateToRestore = new Object[]{new java.util.AbstractMap.SimpleEntry<String, Object>("Value", 15L)};
        countAttributeAggregator.restoreState(stateToRestore);

        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull("Jedis connection should be available for verification", jedisForTest);
            Assert.assertEquals("15", jedisForTest.get(baseRedisKey));
        }
        // Also check local value if jedis was to fail immediately after setting
        Object[] currentState = countAttributeAggregator.currentState(); // This will re-fetch from Redis if connected
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) currentState[0];
        Assert.assertEquals(15L, stateEntry.getValue());
    }

    // --- Fallback Tests ---

    private CountAttributeAggregator initAggregatorForFallbackTest(String planNamePostfix) {
        setupForBadRedis(); // Sets properties for bad Redis and resets pool

        ExecutionPlanContext fallbackEc = new ExecutionPlanContext();
        fallbackEc.setName("TestExecutionPlan_Fallback_" + planNamePostfix);
        fallbackEc.setElementIdGenerator(new TestElementIdGenerator(fallbackEc.getName()));

        CountAttributeAggregator fallbackAggregator = new CountAttributeAggregator();
        // init will now attempt to use RedisConnectionManager, which should fail to connect to "invalid-redis-host"
        // or return null Jedis, triggering fallback in aggregator methods.
        fallbackAggregator.init(null, fallbackEc);
        return fallbackAggregator;
    }

    @Test
    public void testProcessAddFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("ProcessAdd");
        String potentialRedisKey = "siddhi:count:TestExecutionPlan_Fallback_ProcessAdd:element-0";

        Assert.assertEquals(1L, fallbackAggregator.processAdd(new Object()));
        Assert.assertEquals(2L, fallbackAggregator.processAdd(new Object()));

        // Verify no key was created in actual Redis (if a real Redis was somehow available and manager connected to it)
        // This requires getting a connection to the *actual* default Redis to check.
        // This part is tricky if RedisConnectionManager is already configured to a bad host.
        // For true isolation, this check might need a separate Jedis connection not through the manager.
        // However, if initFallbackAggregator correctly configures manager for bad host, manager's getJedis() would fail.
        // Let's assume the fallbackAggregator itself won't be able to write.
        // If we want to be absolutely sure nothing was written to a *good* redis, we'd check the good redis.
        try (Jedis goodJedis = new Jedis(redisHost, redisPort)) {
            goodJedis.connect(); // Direct connection for test verification
            Assert.assertNull("Redis key should not exist for fallback aggregator on the actual Redis", goodJedis.get(potentialRedisKey));
        } catch (Exception e) {
            // If the "good" redis is not running, this check is moot, which is fine.
            System.err.println("Could not connect to good Redis for fallback verification: " + e.getMessage());
        }
    }

    @Test
    public void testProcessRemoveFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("ProcessRemove");
        fallbackAggregator.processAdd(new Object()); // val = 1
        fallbackAggregator.processAdd(new Object()); // val = 2
        fallbackAggregator.processAdd(new Object()); // val = 3

        Assert.assertEquals(2L, fallbackAggregator.processRemove(new Object()));
        Assert.assertEquals(1L, fallbackAggregator.processRemove(new Object()));
    }

    @Test
    public void testResetFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("Reset");

        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());

        Assert.assertEquals(0L, fallbackAggregator.reset());
        Assert.assertEquals(1L, fallbackAggregator.processAdd(new Object())); // Should be 1 after reset
    }

    @Test
    public void testCurrentStateFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("CurrentState");

        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());
        fallbackAggregator.processAdd(new Object());

        Object[] state = fallbackAggregator.currentState();
        Assert.assertNotNull(state);
        Assert.assertEquals(1, state.length);
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        Assert.assertEquals("Value", stateEntry.getKey());
        Assert.assertEquals(3L, stateEntry.getValue());
    }

    @Test
    public void testRestoreStateFallback() {
        CountAttributeAggregator fallbackAggregator = initAggregatorForFallbackTest("RestoreState");

        Object[] stateToRestore = new Object[]{new java.util.AbstractMap.SimpleEntry<String, Object>("Value", 25L)};
        fallbackAggregator.restoreState(stateToRestore);

        Object[] currentState = fallbackAggregator.currentState();
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) currentState[0];
        Assert.assertEquals(25L, stateEntry.getValue());
    }

    // --- Redis Key Uniqueness Test ---
    @Test
    public void testRedisKeyUniqueness() {
        setupForGoodRedis(); // Ensure Redis manager is set for a working Redis

        // Aggregator 1
        ExecutionPlanContext ctx1 = new ExecutionPlanContext();
        ctx1.setName("UniquenessTestPlanA");
        ctx1.setElementIdGenerator(new TestElementIdGenerator(ctx1.getName()));
        CountAttributeAggregator agg1 = new CountAttributeAggregator();
        agg1.init(null, ctx1);
        String key1 = "siddhi:count:UniquenessTestPlanA:element-0";
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull(jedisForTest);
            jedisForTest.del(key1);
        }
        agg1.processAdd(new Object());
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertEquals("1", jedisForTest.get(key1));
        }

        // Aggregator 2 (Different Execution Plan Name)
        ExecutionPlanContext ctx2 = new ExecutionPlanContext();
        ctx2.setName("UniquenessTestPlanB");
        ctx2.setElementIdGenerator(new TestElementIdGenerator(ctx2.getName()));
        CountAttributeAggregator agg2 = new CountAttributeAggregator();
        agg2.init(null, ctx2);
        String key2 = "siddhi:count:UniquenessTestPlanB:element-0";
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            jedisForTest.del(key2);
        }
        agg2.processAdd(new Object());
        agg2.processAdd(new Object());
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertEquals("2", jedisForTest.get(key2));
            Assert.assertEquals("1", jedisForTest.get(key1)); // Ensure key1 is not affected
        }

        // Aggregator 3 (Same Execution Plan Name as Agg1, but different instance, so different elementId)
        CountAttributeAggregator agg3 = new CountAttributeAggregator();
        agg3.init(null, ctx1); // Uses same context as agg1, should get element-1
        String key3 = "siddhi:count:UniquenessTestPlanA:element-1";
         try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            jedisForTest.del(key3);
        }
        agg3.processAdd(new Object());
        agg3.processAdd(new Object());
        agg3.processAdd(new Object());
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertEquals("3", jedisForTest.get(key3));
            Assert.assertEquals("1", jedisForTest.get(key1));
            Assert.assertEquals("2", jedisForTest.get(key2));
        }

        // Cleanup for this specific test
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            jedisForTest.del(key1);
            jedisForTest.del(key2);
            jedisForTest.del(key3);
        }
    }

    @Test
    public void testConcurrentAccessWithSharedPool() {
        setupForGoodRedis(); // Ensure pool is configured for a working Redis

        ExecutionPlanContext ctxA = new ExecutionPlanContext();
        ctxA.setName("ConcurrencyPlanA");
        ctxA.setElementIdGenerator(new TestElementIdGenerator(ctxA.getName()));
        CountAttributeAggregator aggA = new CountAttributeAggregator();
        aggA.init(null, ctxA); // element-0
        String keyA = "siddhi:count:ConcurrencyPlanA:element-0";

        ExecutionPlanContext ctxB = new ExecutionPlanContext();
        ctxB.setName("ConcurrencyPlanB");
        ctxB.setElementIdGenerator(new TestElementIdGenerator(ctxB.getName()));
        CountAttributeAggregator aggB = new CountAttributeAggregator();
        aggB.init(null, ctxB); // element-0
        String keyB = "siddhi:count:ConcurrencyPlanB:element-0";

        ExecutionPlanContext ctxC = new ExecutionPlanContext();
        // Same plan name as A, but will get a new element ID from its own generator instance within the context
        ctxC.setName("ConcurrencyPlanA");
        ctxC.setElementIdGenerator(new TestElementIdGenerator(ctxC.getName())); // Fresh generator for this context
        CountAttributeAggregator aggC = new CountAttributeAggregator();
        aggC.init(null, ctxC); // element-0 (because of fresh generator for ctxC)
        // String keyC = "siddhi:count:ConcurrencyPlanA:element-0"; // This would collide with keyA if names were identical AND elementId wasn't unique
                                                                // But since ctxC has its own ElementIdGenerator, it will also start with element-0
                                                                // So the key will be the same as keyA if plan name is same.
                                                                // Let's make plan name unique for aggC for this test to avoid confusion with aggA's elementId from ctxA.
                                                                // Or, more simply, ensure aggC uses ctxA to get a new ID from *that* context's generator.
        // Re-thinking aggC for clarity:
        // To test shared pool with distinct keys from the *same* plan but different aggregator instances:
        CountAttributeAggregator aggA_instance2 = new CountAttributeAggregator();
        aggA_instance2.init(null, ctxA); // ctxA's generator has used element-0, so this gets element-1
        String keyA_instance2 = "siddhi:count:ConcurrencyPlanA:element-1";


        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull(jedisForTest);
            jedisForTest.del(keyA);
            jedisForTest.del(keyB);
            jedisForTest.del(keyA_instance2);
        }

        // Interleaved operations
        aggA.processAdd(new Object()); // A:1
        aggB.processAdd(new Object()); // B:1
        aggA_instance2.processAdd(new Object()); // A_instance2:1

        aggA.processAdd(new Object()); // A:2
        aggB.processAdd(new Object()); // B:2
        aggA.processAdd(new Object()); // A:3

        aggA_instance2.processAdd(new Object()); // A_instance2:2
        aggB.processRemove(new Object()); // B:1

        // Verify final counts
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull(jedisForTest);
            Assert.assertEquals("3", jedisForTest.get(keyA));
            Assert.assertEquals("1", jedisForTest.get(keyB));
            Assert.assertEquals("2", jedisForTest.get(keyA_instance2));
        }

        // Cleanup
        try (Jedis jedisForTest = org.wso2.siddhi.core.util.persistence.RedisConnectionManager.getJedis()) {
            Assert.assertNotNull(jedisForTest);
            jedisForTest.del(keyA);
            jedisForTest.del(keyB);
            jedisForTest.del(keyA_instance2);
        }
    }
}
