package io.siddhi.core.aggregation.dbbaseaggregation.config;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This class contains all the Siddhi RDBMS Event Table SQL query configuration mappings.
 */
@XmlRootElement(name = "database")
public class DBAggregationQueryConfigurationEntry {

    private String databaseName;
    private String category;
    private double minVersion;
    private double maxVersion;
    private String stringSize;
    private int fieldSizeLimit;
    private RDBMSTypeMapping rdbmsTypeMapping;
    private RDBMSSelectQueryTemplate rdbmsSelectQueryTemplate;
    private RDBMSSelectFunctionTemplate rdbmsSelectFunctionTemplate;
    private int batchSize;
    private boolean batchEnable = false;
    private String collation;
    private boolean transactionSupported = true;

    @XmlAttribute(name = "name", required = true)
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @XmlAttribute(name = "minVersion")
    public double getMinVersion() {
        return minVersion;
    }

    public void setMinVersion(double minVersion) {
        this.minVersion = minVersion;
    }

    @XmlAttribute(name = "maxVersion")
    public double getMaxVersion() {
        return maxVersion;
    }

    public void setMaxVersion(double maxVersion) {
        this.maxVersion = maxVersion;
    }

    @XmlAttribute(name = "category")
    public String getCategory() {
        return category;
    }

    @XmlElement(name = "batchEnable")
    public boolean getBatchEnable() {
        return batchEnable;
    }

    public void setBatchEnable(boolean batchEnable) {
        this.batchEnable = batchEnable;
    }

    @XmlElement(name = "collation")
    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getStringSize() {
        return stringSize;
    }

    public void setStringSize(String stringSize) {
        this.stringSize = stringSize;
    }

    public boolean isTransactionSupported() {
        return transactionSupported;
    }

    public void setTransactionSupported(boolean transactionSupported) {
        this.transactionSupported = transactionSupported;
    }

    @XmlElement(name = "typeMapping", required = true)
    public RDBMSTypeMapping getRdbmsTypeMapping() {
        return rdbmsTypeMapping;
    }

    public void setRdbmsTypeMapping(RDBMSTypeMapping rdbmsTypeMapping) {
        this.rdbmsTypeMapping = rdbmsTypeMapping;
    }

    @XmlElement(name = "selectQueryTemplate")
    public RDBMSSelectQueryTemplate getRdbmsSelectQueryTemplate() {
        return rdbmsSelectQueryTemplate;
    }

    public void setRdbmsSelectQueryTemplate(RDBMSSelectQueryTemplate rdbmsSelectQueryTemplate) {
        this.rdbmsSelectQueryTemplate = rdbmsSelectQueryTemplate;
    }

    @XmlElement(name = "batchSize", required = true)
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @XmlElement(name = "fieldSizeLimit", required = false)
    public int getFieldSizeLimit() {
        return fieldSizeLimit;
    }

    public void setFieldSizeLimit(int fieldSizeLimit) {
        this.fieldSizeLimit = fieldSizeLimit;
    }

    @XmlElement(name = "selectQueryFunctions")
    public RDBMSSelectFunctionTemplate getRdbmsSelectFunctionTemplate() {
        return rdbmsSelectFunctionTemplate;
    }

    public void setRdbmsSelectFunctionTemplate(RDBMSSelectFunctionTemplate rdbmsSelectFunctionTemplate) {
        this.rdbmsSelectFunctionTemplate = rdbmsSelectFunctionTemplate;
    }
}

