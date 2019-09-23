package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "range-partitioning", propOrder = { "partitionColumn", "partition" })
public class RangePartitioning
{
    @XmlElement(name = "partition-column", required = true)
    protected Column partitionColumn;
    protected List<RangePartition> partition;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "union-unpartitionable-queries")
    protected Boolean unionUnpartitionableQueries;
    @XmlAttribute(name = "partition-value-type")
    protected String partitionValueType;
    
    public Column getPartitionColumn() {
        return this.partitionColumn;
    }
    
    public void setPartitionColumn(final Column value) {
        this.partitionColumn = value;
    }
    
    public List<RangePartition> getPartition() {
        if (this.partition == null) {
            this.partition = new ArrayList<RangePartition>();
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
    
    public String getPartitionValueType() {
        return this.partitionValueType;
    }
    
    public void setPartitionValueType(final String value) {
        this.partitionValueType = value;
    }
}
