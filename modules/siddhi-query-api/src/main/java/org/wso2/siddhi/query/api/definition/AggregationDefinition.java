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
package org.wso2.siddhi.query.api.definition;

import org.wso2.siddhi.query.api.aggregation.TimePeriod;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.selection.BasicSelector;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Variable;

public class AggregationDefinition extends AbstractDefinition {

    private InputStream inputStream = null;
    private Selector selector = null;
    private Variable aggregateAttribute = null;
    private TimePeriod timePeriod = null;
    private Annotation annotation = null;


    protected AggregationDefinition(String id) {
        super(id);
    }

    public AggregationDefinition select(BasicSelector selector) {
//        if(selector.getHavingExpression()!=null){
//            throw new OperationNotSupportedException ("Having condition cannot be added for Aggregate Definition," +
//                    " but found in '"+id+"'");
//        }
        this.selector = selector;
        return this;
    }

    public Selector getSelector() {
        return this.selector;
    }

    public static AggregationDefinition id(String aggregationName) {
        return new AggregationDefinition(aggregationName);
    }

    public AggregationDefinition aggregateBy(Variable aggregateAttribute) {
        this.aggregateAttribute = aggregateAttribute;
        return this;
    }

    public Variable getAggregateAttribute() {
        return this.aggregateAttribute;
    }

    public AggregationDefinition every(TimePeriod timePeriod) {
        this.timePeriod = timePeriod;
        return this;
    }

    public TimePeriod getTimePeriod() {
        return this.timePeriod;
    }

    public AggregationDefinition from(InputStream inputStream) {
        this.inputStream = inputStream;
        return this;
    }

    public InputStream getInputStream() {
        return this.inputStream;
    }

    public AggregationDefinition annotation(Annotation annotation) {
        this.annotation = annotation;
        return this;
    }

    public Annotation getAnnotation() {
        return this.annotation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AggregationDefinition that = (AggregationDefinition) o;

        if (!inputStream.equals(that.inputStream)) return false;
        if (selector != null ? !selector.equals(that.selector) : that.selector != null) return false;
        if (!aggregateAttribute.equals(that.aggregateAttribute)) return false;
        if (timePeriod != null ? !timePeriod.equals(that.timePeriod) : that.timePeriod != null) return false;
        return annotation != null ? annotation.equals(that.annotation) : that.annotation == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + inputStream.hashCode();
        result = 31 * result + (selector != null ? selector.hashCode() : 0);
        result = 31 * result + aggregateAttribute.hashCode();
        result = 31 * result + (timePeriod != null ? timePeriod.hashCode() : 0);
        result = 31 * result + (annotation != null ? annotation.hashCode() : 0);
        return result;
    }
}
