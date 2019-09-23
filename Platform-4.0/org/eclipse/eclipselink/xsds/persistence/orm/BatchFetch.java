package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.math.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "batch-fetch")
public class BatchFetch
{
    @XmlAttribute(name = "type")
    protected BatchFetchType type;
    @XmlAttribute(name = "size")
    protected BigInteger size;
    
    public BatchFetchType getType() {
        return this.type;
    }
    
    public void setType(final BatchFetchType value) {
        this.type = value;
    }
    
    public BigInteger getSize() {
        return this.size;
    }
    
    public void setSize(final BigInteger value) {
        this.size = value;
    }
}
