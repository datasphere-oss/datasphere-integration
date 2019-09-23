package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "range-partition")
public class RangePartition
{
    @XmlAttribute(name = "start-value")
    protected String startValue;
    @XmlAttribute(name = "end-value")
    protected String endValue;
    @XmlAttribute(name = "connection-pool", required = true)
    protected String connectionPool;
    
    public String getStartValue() {
        return this.startValue;
    }
    
    public void setStartValue(final String value) {
        this.startValue = value;
    }
    
    public String getEndValue() {
        return this.endValue;
    }
    
    public void setEndValue(final String value) {
        this.endValue = value;
    }
    
    public String getConnectionPool() {
        return this.connectionPool;
    }
    
    public void setConnectionPool(final String value) {
        this.connectionPool = value;
    }
}
