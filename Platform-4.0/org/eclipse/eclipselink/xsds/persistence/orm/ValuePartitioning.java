package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "value-partitioning", propOrder = { "partitionColumn", "partition" })
public class ValuePartitioning
{
    @XmlElement(name = "partition-column", required = true)
    protected Column partitionColumn;
    @XmlElement(required = true)
    protected List<ValuePartition> partition;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "union-unpartitionable-queries")
    protected Boolean unionUnpartitionableQueries;
    @XmlAttribute(name = "default-connection-pool")
    protected String defaultConnectionPool;
    @XmlAttribute(name = "partition-value-type")
    protected String partitionValueType;
    
    public Column getPartitionColumn() {
        return this.partitionColumn;
    }
    
    public void setPartitionColumn(final Column value) {
        this.partitionColumn = value;
    }
    
    public List<ValuePartition> getPartition() {
        if (this.partition == null) {
            this.partition = new ArrayList<ValuePartition>();
        }
        return this.partition;
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
    
    public String getDefaultConnectionPool() {
        return this.defaultConnectionPool;
    }
    
    public void setDefaultConnectionPool(final String value) {
        this.defaultConnectionPool = value;
    }
    
    public String getPartitionValueType() {
        return this.partitionValueType;
    }
    
    public void setPartitionValueType(final String value) {
        this.partitionValueType = value;
    }
}
