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

package org.wso2.siddhi.extension.math;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class CeilingFunctionExtensionTestCase {
    private static Log logger = LogFactory.getLog(CeilingFunctionExtensionTestCase.class);
    protected static SiddhiManager siddhiManager;
    private boolean eventArrived;

    @Test
    public void testProcess() throws Exception {
        logger.info("CeilingFunctionExtension TestCase");

        siddhiManager = new SiddhiManager();
        String inValueStream = "define stream InValueStream (inValue double);";

        String eventFuseExecutionPlan = ("@info(name = 'query1') from InValueStream "
                + "select math:ceil(inValue) as ceilingValue "
                + "insert into OutMediationStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inValueStream +
                                                                                             eventFuseExecutionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Double result;
                for (Event event : inEvents) {
                    result = (Double) event.getData(0);
                    Assert.assertEquals((Double) 424.0, result);
                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime
                .getInputHandler("InValueStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{423.187d});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void exceptionTestCase1() throws Exception {
        logger.info("CeilingFunctionExtension exceptionTestCase1");

        siddhiManager = new SiddhiManager();
        String inValueStream = "define stream InValueStream (inValue double);";

        String eventFuseExecutionPlan = ("@info(name = 'query1') from InValueStream "
                                         + "select math:ceil(inValue,inValue) as ceilingValue "
                                         + "insert into OutMediationStream;");
        siddhiManager.createExecutionPlanRuntime(inValueStream + eventFuseExecutionPlan);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void exceptionTestCase2() throws Exception {
        logger.info("CeilingFunctionExtension exceptionTestCase2");

        siddhiManager = new SiddhiManager();
        String inValueStream = "define stream InValueStream (inValue object);";

        String eventFuseExecutionPlan = ("@info(name = 'query1') from InValueStream "
                                         + "select math:ceil(inValue) as ceilingValue "
                                         + "insert into OutMediationStream;");
        siddhiManager.createExecutionPlanRuntime(inValueStream + eventFuseExecutionPlan);
    }

    @Test
    public void exceptionTestCase3() throws Exception {
        logger.info("CeilingFunctionExtension exceptionTestCase3");

        siddhiManager = new SiddhiManager();
        String inValueStream = "define stream InValueStream (inValue double);";

        String eventFuseExecutionPlan = ("@info(name = 'query1') from InValueStream "
                                         + "select math:ceil(inValue) as ceilingValue "
                                         + "insert into OutMediationStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inValueStream +
                                                                                             eventFuseExecutionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }
        });
        InputHandler inputHandler = executionPlanRuntime
                .getInputHandler("InValueStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{null});
        Thread.sleep(100);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testProcessFloat() throws Exception {
        logger.info("CeilingFunctionExtension testProcessFloat");

        siddhiManager = new SiddhiManager();
        String inValueStream = "define stream InValueStream (inValue float);";

        String eventFuseExecutionPlan = ("@info(name = 'query1') from InValueStream "
                                         + "select math:ceil(inValue) as ceilingValue "
                                         + "insert into OutMediationStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inValueStream +
                                                                                             eventFuseExecutionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Double result;
                for (Event event : inEvents) {
                    result = (Double) event.getData(0);
                    Assert.assertEquals((Double) 424.0, result);
                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime
                .getInputHandler("InValueStream");
        executionPlanRuntime.start();
        inputHandler.send(new Float[]{423.187f});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
    }
}
