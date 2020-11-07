package io.siddhi.core.aggregation.persistedaggregation.config;

import io.siddhi.query.api.aggregation.TimePeriod;

public class DBAggregationTimeConversionDurationMapping {
    private String day;
    private String month;
    private String year;



    public void setMonth(String month) {
        this.month = month;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getDurationMapping(TimePeriod.Duration duration){
        switch (duration){
            case DAYS:
                return day;
            case MONTHS:
                return month;
            case YEARS:
                return year;
            default:
                //this won't get hit since persisted aggregators only will be created days and upward.
                return "";
        }
    }
}
