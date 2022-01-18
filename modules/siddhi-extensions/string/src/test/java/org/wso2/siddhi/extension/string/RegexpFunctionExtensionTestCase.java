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

package org.wso2.siddhi.extension.string;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.string.test.util.SiddhiTestHelper;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class RegexpFunctionExtensionTestCase {
    static final Log log = LogFactory.getLog(RegexpFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testRegexpFunctionExtension1() throws InterruptedException {
        log.info("RegexpFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string);";
        String query = ("@info(name = 'query1') from inputStream select symbol ,"
                + " str:regexp(symbol, regex) as beginsWithWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(false, event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(false, event.getData(1));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"hello hi hello", 700f, "^WSO2(.*)"});
        inputHandler.send(new Object[]{"WSO2 abcdh", 60.5f, "WSO(.*h)"});
        inputHandler.send(new Object[]{"aaWSO2 hi hello", 60.5f, "^WSO2(.*)"});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testRegexpFunctionExtension2() throws InterruptedException {
        log.info("RegexpFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex int);";
        String query = ("@info(name = 'query1') from inputStream select symbol , "
                + "str:regexp(symbol, '^WSO2(.*)') as beginsWithWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(false, event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(false, event.getData(1));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"hello hi hello", 700f, 1});
        inputHandler.send(new Object[]{"WSO2 abcdh", 60.5f, 1});
        inputHandler.send(new Object[]{"aaWSO2 hi hello", 60.5f, 1});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testRegexpFunctionExtension3() throws InterruptedException {
        log.info("RegexpFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , "
                + "str:regexp(symbol) as beginsWithWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testRegexpFunctionExtension4() throws InterruptedException {
        log.info("RegexpFunctionExtension TestCase with invalid datatype");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol int, price long, regex string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , "
                + "str:regexp(symbol, regex) as beginsWithWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testRegexpFunctionExtension5() throws InterruptedException {
        log.info("RegexpFunctionExtension TestCase with invalid datatype");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex int);";
        String query = ("@info(name = 'query1') from inputStream select symbol , "
                + "str:regexp(symbol, regex) as beginsWithWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testRegexpFunctionExtension6() throws InterruptedException {
        log.info("RegexpFunctionExtension TestCase with null value");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , "
                + "str:regexp(symbol, regex) as beginsWithWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{null, 700f, "^WSO2(.*)"});
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testRegexpFunctionExtension7() throws InterruptedException {
        log.info("RegexpFunctionExtension TestCase with null value");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , "
                + "str:regexp(symbol, regex) as beginsWithWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2 abcdh", 700f, null});
        executionPlanRuntime.shutdown();
    }
}
