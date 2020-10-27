package io.siddhi.core.aggregation.dbbaseaggregation.config;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This class represents the JAXB bean for the query configuration which provide a link between RDBMS Event Table
 * functions and DB vendor-specific SQL syntax.
 */
@XmlRootElement(name = "rdbms-table-configuration")
public class DBAggregationQueryConfiguration {

    private DBAggregationQueryConfigurationEntry[] databases;

    @XmlElement(name = "database")
    public DBAggregationQueryConfigurationEntry[] getDatabases() {
        return databases;
    }

    public void setDatabases(DBAggregationQueryConfigurationEntry[] databases) {
        this.databases = databases;
    }

}
