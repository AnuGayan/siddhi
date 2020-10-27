package io.siddhi.core.aggregation.dbbaseaggregation.config;

/**
 * This class represents clauses of a SELECT query as required by Siddhi RDBMS Event Tables per supported DB vendor.
 */
public class RDBMSSelectQueryTemplate {

    private String selectClause;
    private String recordInsertQuery;
    private String selectQueryWithSubSelect;
    private String whereClause;
    private String groupByClause;
    private String havingClause;
    private String orderByClause;
    private String limitClause;
    private String offsetClause;
    private String queryWrapperClause;
    private String limitWrapperClause;
    private String offsetWrapperClause;
    private String isLimitBeforeOffset;

    public String getSelectClause() {
        return selectClause;
    }

    public void setSelectClause(String selectClause) {
        this.selectClause = selectClause;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public String getGroupByClause() {
        return groupByClause;
    }

    public void setGroupByClause(String groupByClause) {
        this.groupByClause = groupByClause;
    }

    public String getHavingClause() {
        return havingClause;
    }

    public void setHavingClause(String havingClause) {
        this.havingClause = havingClause;
    }

    public String getOrderByClause() {
        return orderByClause;
    }

    public void setOrderByClause(String orderByClause) {
        this.orderByClause = orderByClause;
    }

    public String getLimitClause() {
        return limitClause;
    }

    public void setLimitClause(String limitClause) {
        this.limitClause = limitClause;
    }

    public String getOffsetClause() {
        return offsetClause;
    }

    public void setOffsetClause(String offsetClause) {
        this.offsetClause = offsetClause;
    }

    public String getIsLimitBeforeOffset() {
        return isLimitBeforeOffset;
    }

    public void setIsLimitBeforeOffset(String isLimitBeforeOffset) {
        this.isLimitBeforeOffset = isLimitBeforeOffset;
    }

    public String getQueryWrapperClause() {
        return queryWrapperClause;
    }

    public void setQueryWrapperClause(String queryWrapperClause) {
        this.queryWrapperClause = queryWrapperClause;
    }

    public String getLimitWrapperClause() {
        return limitWrapperClause;
    }

    public void setLimitWrapperClause(String limitWrapperClause) {
        this.limitWrapperClause = limitWrapperClause;
    }

    public String getOffsetWrapperClause() {
        return offsetWrapperClause;
    }

    public void setOffsetWrapperClause(String offsetWrapperClause) {
        this.offsetWrapperClause = offsetWrapperClause;
    }

    public String getSelectQueryWithSubSelect() {
        return selectQueryWithSubSelect;
    }

    public void setSelectQueryWithSubSelect(String selectQueryWithSubSelect) {
        this.selectQueryWithSubSelect = selectQueryWithSubSelect;
    }

    public String getRecordInsertQuery() {
        return recordInsertQuery;
    }

    public void setRecordInsertQuery(String recordInsertQuery) {
        this.recordInsertQuery = recordInsertQuery;
    }
}
