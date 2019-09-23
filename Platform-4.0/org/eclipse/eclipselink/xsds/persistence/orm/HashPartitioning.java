package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "hash-partitioning", propOrder = { "partitionColumn", "connectionPool" })
public class HashPartitioning
{
    @XmlElement(name = "partition-column", required = true)
    protected Column partitionColumn;
    @XmlElement(name = "connection-pool")
    protected List<String> connectionPool;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "union-unpartitionable-queries")
    protected Boolean unionUnpartitionableQueries;
    
    public Column getPartitionColumn() {
        return this.partitionColumn;
    }
    
    public void setPartitionColumn(final Column value) {
        this.partitionColumn = value;
    }
    
    public List<String> getConnectionPool() {
        if (this.connectionPool == null) {
            this.connectionPool = new ArrayList<String>();
        }
        return this.connectionPool;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public Boolean isUnionUnpartitionableQueries() {
        return this.unionUnpartitionableQueries;
    }
    
    public void setUnionUnpartitionableQueries(final Boolean value) {
        this.unionUnpartitionableQueries = value;
    }
}
