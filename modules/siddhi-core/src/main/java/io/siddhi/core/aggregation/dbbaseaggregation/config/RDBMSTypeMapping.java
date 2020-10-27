package io.siddhi.core.aggregation.dbbaseaggregation.config;

/**
 * This class represents the SQL datatype mappings required by Siddhi RDBMS Event Tables per supported DB vendor.
 */
public class RDBMSTypeMapping {

    // -- Type mapping -- //
    private String binaryType;
    private String booleanType;
    private String doubleType;
    private String floatType;
    private String integerType;
    private String longType;
    private String stringType;
    private String bigStringType;

    public String getBinaryType() {
        return binaryType;
    }

    public void setBinaryType(String binaryType) {
        this.binaryType = binaryType;
    }

    public String getBooleanType() {
        return booleanType;
    }

    public void setBooleanType(String booleanType) {
        this.booleanType = booleanType;
    }

    public String getDoubleType() {
        return doubleType;
    }

    public void setDoubleType(String doubleType) {
        this.doubleType = doubleType;
    }

    public String getFloatType() {
        return floatType;
    }

    public void setFloatType(String floatType) {
        this.floatType = floatType;
    }

    public String getIntegerType() {
        return integerType;
    }

    public void setIntegerType(String integerType) {
        this.integerType = integerType;
    }

    public String getLongType() {
        return longType;
    }

    public void setLongType(String longType) {
        this.longType = longType;
    }

    public String getStringType() {
        return stringType;
    }

    public void setStringType(String stringType) {
        this.stringType = stringType;
    }

    public String getBigStringType() {
        return bigStringType;
    }

    public void setBigStringType(String bigStringType) {
        this.bigStringType = bigStringType;
    }
}

