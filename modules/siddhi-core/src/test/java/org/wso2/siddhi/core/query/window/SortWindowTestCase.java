/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.query.window;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.test.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class SortWindowTestCase {
    private static final Log log = LogFactory.getLog(SortWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private AtomicInteger atomicCount;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        atomicCount = new AtomicInteger(0);
    }

    @Test
    public void sortWindowTest1() throws InterruptedException {
        log.info("sortWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.sort(2,volume, 'asc') " +
                "select volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 55.6f, 100l});
        inputHandler.send(new Object[]{"IBM", 75.6f, 300l});
        inputHandler.send(new Object[]{"WSO2", 57.6f, 200l});
        inputHandler.send(new Object[]{"WSO2", 55.6f, 20l});
        inputHandler.send(new Object[]{"WSO2", 57.6f, 40l});
        Thread.sleep(1000);
        Assert.assertEquals(5, inEventCount);
        Assert.assertEquals(3, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void sortWindowTest2() throws InterruptedException {
        log.info("sortWindow test2");

        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@plan:name('sortWindow2') ";
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price int, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.sort(2,volume, 'asc', price, 'desc') " +
                "select price, volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(planName + cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 50, 100l});
        inputHandler.send(new Object[]{"IBM", 20, 100l});
        inputHandler.send(new Object[]{"WSO2", 40, 50l});
        inputHandler.send(new Object[]{"WSO2", 100, 20l});
        inputHandler.send(new Object[]{"WSO2", 50, 50l});
        Thread.sleep(1000);
        Assert.assertEquals(5, inEventCount);
        Assert.assertEquals(3, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void sortWindowTest3() throws InterruptedException {
        log.info("sortWindowTest3");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, index int); " +
                "define stream twitterStream (id int, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.sort(2, index) join twitterStream#window.sort(2, id) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        atomicCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount+=(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            cseEventStreamHandler.send(new Object[]{"IBM", 59.6f, 101});
            twitterStreamHandler.send(new Object[]{10, "Hello World", "WSO2"});
            twitterStreamHandler.send(new Object[]{15, "Hello World2", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 90});
            twitterStreamHandler.send(new Object[]{5, "Hello World2", "IBM"});
            SiddhiTestHelper.waitForEvents(100, 3, atomicCount, 10000);
            Assert.assertEquals(3, atomicCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void sortWindowTest4() throws InterruptedException {
        log.info("sortWindowTest4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.sort(2.5) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        String message = "";
        try {
            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        } catch (ExecutionPlanValidationException e) {
            message = e.getMessage();
        }
        Assert.assertTrue(message.contains("The first parameter should be an integer"));
    }

    @Test
    public void sortWindowTest5() throws InterruptedException {
        log.info("sortWindowTest5");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, time long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.sort(2, 8) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        String message = "";
        try {
            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        } catch (ExecutionPlanValidationException e) {
            message = e.getMessage();
        }
        Assert.assertTrue(message.contains("Required a variable, but found a string parameter"));
    }

    @Test
    public void sortWindowTest6() throws InterruptedException {
        log.info("sortWindowTest6");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, time long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.sort(2, volume, 'ecs') " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        String message = "";
        try {
            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        } catch (ExecutionPlanValidationException e) {
            message = e.getMessage();
        }
        Assert.assertTrue( message.contains("Parameter string literals should only be \"asc\" or \"desc\""));
    }
}
