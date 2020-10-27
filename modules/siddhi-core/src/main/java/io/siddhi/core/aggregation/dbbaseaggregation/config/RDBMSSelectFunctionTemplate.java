package io.siddhi.core.aggregation.dbbaseaggregation.config;

/**
 * This class contains Select functions query configuration mappings.
 */
public class RDBMSSelectFunctionTemplate {
    private String sumFunction;
    private String countFunction;
    private String maxFunction;


    public String getSumFunction() {
        return sumFunction;
    }

    public void setSumFunction(String sumFunction) {
        this.sumFunction = sumFunction;
    }

    public String getCountFunction() {
        return countFunction;
    }

    public void setCountFunction(String countFunction) {
        this.countFunction = countFunction;
    }

    public String getMaxFunction() {
        return maxFunction;
    }

    public void setMaxFunction(String maxFunction) {
        this.maxFunction = maxFunction;
    }
}
